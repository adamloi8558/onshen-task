import { Job } from 'bullmq';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import fs from 'fs-extra';
import path from 'path';
import { exec } from 'child_process';
import { promisify } from 'util';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { updateContentVideo } from '../db';
import { logger } from '../utils/logger';

const execPromise = promisify(exec);

// Set ffmpeg path
if (ffmpegStatic) {
  ffmpeg.setFfmpegPath(ffmpegStatic);
}

// S3 Client
const s3Client = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID!,
    secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY!,
  },
});

const BUCKET_NAME = process.env.CLOUDFLARE_R2_BUCKET_NAME!;
const PUBLIC_URL = process.env.CLOUDFLARE_R2_PUBLIC_URL!;

export interface YoutubeDownloadJob {
  jobId: string;
  userId: string;
  contentId: string;
  youtubeUrl: string;
  videoId: string;
  title: string;
  cookies?: string;
}

export async function processYoutubeDownload(job: Job<YoutubeDownloadJob>): Promise<void> {
  const { jobId, contentId, youtubeUrl, videoId, title, cookies } = job.data;
  
  logger.info('Starting YouTube download', { 
    jobId, 
    videoId, 
    youtubeUrl,
    hasCookies: !!cookies 
  });
  await job.updateProgress(5);

  const tempDir = path.join(process.cwd(), 'temp', jobId);
  const outputDir = path.join(tempDir, 'output');
  
  try {
    // Create temp directories
    await fs.ensureDir(tempDir);
    await fs.ensureDir(outputDir);
    
    // Create cookies file if provided
    const cookiesFilePath = cookies ? path.join(tempDir, 'cookies.txt') : null;
    if (cookies && cookiesFilePath) {
      logger.info('Creating cookies file for authentication');
      
      // Convert cookies string to Netscape format
      const cookiesContent = `# Netscape HTTP Cookie File
# This file is generated for yt-dlp
.youtube.com	TRUE	/	TRUE	0	__Secure-1PSID	${cookies}`;
      
      await fs.writeFile(cookiesFilePath, cookiesContent);
      logger.info('Cookies file created');
    }

    // Step 1: Download video using yt-dlp
    logger.info('Downloading video from YouTube');
    const videoPath = path.join(tempDir, 'video.mp4');
    
    // Using yt-dlp with cookies if provided
    const cookiesArg = cookiesFilePath ? `--cookies "${cookiesFilePath}"` : '';
    
    const downloadStrategies = [
      // Strategy 1: With cookies (if provided)
      cookiesFilePath ? 
        `yt-dlp ${cookiesArg} --no-check-certificates -f "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=720]/best" --merge-output-format mp4 --no-playlist --retries 10 -o "${videoPath}" "${youtubeUrl}"` 
        : null,
      
      // Strategy 2: Use iOS client
      `yt-dlp --no-check-certificates --extractor-args "youtube:player_client=ios" -f "best[ext=mp4][height<=720]/best" --no-playlist --retries 5 -o "${videoPath}" "${youtubeUrl}"`,
      
      // Strategy 3: Use Android client
      `yt-dlp --no-check-certificates --extractor-args "youtube:player_client=android" -f "best[ext=mp4][height<=720]/best" --no-playlist --retries 5 -o "${videoPath}" "${youtubeUrl}"`,
      
      // Strategy 4: Simple fallback
      `yt-dlp --no-check-certificates -f "best[ext=mp4][height<=720]/best" --no-playlist -o "${videoPath}" "${youtubeUrl}"`,
    ].filter(Boolean) as string[];
    
    let downloadSuccess = false;
    let lastError: any = null;
    
    for (let i = 0; i < downloadStrategies.length; i++) {
      const strategy = downloadStrategies[i];
      logger.info(`Trying download strategy ${i + 1}/${downloadStrategies.length}`);
      
      await job.updateProgress(10 + i * 5);

      try {
        const { stdout, stderr } = await execPromise(strategy, {
          maxBuffer: 1024 * 1024 * 100, // 100MB buffer
          timeout: 300000, // 5 minutes timeout
        });
        
        logger.info(`Strategy ${i + 1} succeeded!`, { stdout: stdout.substring(0, 300) });
        if (stderr && !stderr.includes('ERROR')) {
          logger.warn('Download stderr:', { stderr: stderr.substring(0, 300) });
        }
        
        // Check if file was actually downloaded
        if (await fs.pathExists(videoPath)) {
          const stats = await fs.stat(videoPath);
          if (stats.size > 1024 * 100) { // At least 100KB
            downloadSuccess = true;
            logger.info('Download verified', { fileSize: `${Math.round(stats.size / 1024 / 1024)}MB` });
            break;
          }
        }
      } catch (strategyError: any) {
        lastError = strategyError;
        logger.warn(`Strategy ${i + 1} failed, trying next...`, { 
          error: strategyError.message?.substring(0, 200),
        });
        
        // Clean up failed download
        if (await fs.pathExists(videoPath)) {
          await fs.remove(videoPath);
        }
        
        // Continue to next strategy
        continue;
      }
    }
    
    if (!downloadSuccess) {
      logger.error('All download strategies failed', { 
        error: lastError?.message,
        stderr: lastError?.stderr?.substring(0, 500),
      });
      throw new Error('ไม่สามารถดาวน์โหลดจาก YouTube ได้ (ลองทุกวิธีแล้ว)');
    }

    await job.updateProgress(40);

    // Step 2: Convert to HLS
    logger.info('Converting to HLS');
    const hlsOutputDir = path.join(outputDir, 'hls');
    await fs.ensureDir(hlsOutputDir);
    
    const playlistPath = path.join(hlsOutputDir, 'playlist.m3u8');
    
    await new Promise<void>((resolve, reject) => {
      ffmpeg(videoPath)
        .outputOptions([
          '-c:v libx264',
          '-preset veryfast',
          '-crf 23',
          '-maxrate 2500k',
          '-bufsize 5000k',
          '-s 1280x720',
          '-c:a aac',
          '-b:a 128k',
          '-ac 2',
          '-ar 44100',
          '-start_number 0',
          '-hls_time 6',
          '-hls_list_size 0',
          '-hls_segment_filename', path.join(hlsOutputDir, 'segment_%03d.ts'),
          '-f hls'
        ])
        .output(playlistPath)
        .on('start', (cmdline) => {
          logger.info('FFmpeg started', { command: cmdline });
        })
        .on('progress', async (progress) => {
          const percent = Math.min(85, 40 + (progress.percent || 0) * 0.45);
          await job.updateProgress(Math.round(percent));
          logger.info('HLS conversion progress', { percent: progress.percent });
        })
        .on('end', () => {
          logger.info('HLS conversion completed');
          resolve();
        })
        .on('error', (err) => {
          logger.error('FFmpeg error', { error: err });
          reject(err);
        })
        .run();
    });

    await job.updateProgress(90);

    // Step 3: Upload HLS files to S3
    logger.info('Uploading HLS files to S3');
    const hlsFiles = await fs.readdir(hlsOutputDir);
    const uploadPromises = hlsFiles.map(async (file) => {
      const filePath = path.join(hlsOutputDir, file);
      const s3Key = `videos/${contentId}/${file}`;
      
      const fileContent = await fs.readFile(filePath);
      const contentType = file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T';
      
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: s3Key,
        Body: fileContent,
        ContentType: contentType,
        CacheControl: file.endsWith('.m3u8') ? 'no-cache' : 'public, max-age=31536000',
      }));
      
      return s3Key;
    });

    await Promise.all(uploadPromises);
    
    // Get the playlist URL
    const playlistS3Key = `videos/${contentId}/playlist.m3u8`;
    const playlistUrl = `${PUBLIC_URL}/${playlistS3Key}`;

    await job.updateProgress(95);

    // Step 4: Update database with video URL
    await updateContentVideo(contentId, playlistUrl);

    await job.updateProgress(100);

    logger.info('YouTube download and conversion completed', {
      jobId,
      contentId,
      playlistUrl,
      hlsFileCount: hlsFiles.length,
    });

  } catch (error) {
    logger.error('YouTube processing failed', { error, jobId });
    throw error;
  } finally {
    // Clean up temp files
    await fs.remove(tempDir);
  }
}
