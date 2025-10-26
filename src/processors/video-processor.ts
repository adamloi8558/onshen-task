import { Job } from 'bullmq';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import fs from 'fs-extra';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { updateUploadJobStatus, updateContentVideo, updateEpisodeVideo } from '../db';
import { logger } from '../utils/logger';
import { VideoUploadJob } from '../index';

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

export async function processVideoUpload(job: Job<VideoUploadJob['data']>): Promise<void> {
  const { jobId, userId, contentId, episodeId, originalFilename, fileSize, uploadPath } = job.data;
  
  // Update job status to processing
  await updateUploadJobStatus(jobId, 'processing', 10);
  await job.updateProgress(10);

  const tempDir = path.join(process.cwd(), 'temp', jobId);
  const outputDir = path.join(tempDir, 'output');
  
  try {
    // Create temp directories
    await fs.ensureDir(tempDir);
    await fs.ensureDir(outputDir);

    // Download original file from S3
    logger.info('Downloading original video file from S3', { uploadPath });
    const tempInputFile = path.join(tempDir, 'input.mp4');
    await downloadFromS3(uploadPath, tempInputFile);
    
    await updateUploadJobStatus(jobId, 'processing', 20);
    await job.updateProgress(20);

    // Generate HLS playlist with single quality (720p) for faster processing
    logger.info('Starting video conversion to HLS (720p only for speed)');
    const hlsOutputDir = path.join(outputDir, 'hls');
    await fs.ensureDir(hlsOutputDir);
    
    const playlistPath = path.join(hlsOutputDir, 'playlist.m3u8');
    
    await new Promise<void>((resolve, reject) => {
      ffmpeg(tempInputFile)
        .outputOptions([
          '-c:v libx264',
          '-preset veryfast', // Faster encoding
          '-crf 23', // Good quality
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
          const percent = Math.min(80, 20 + (progress.percent || 0) * 0.6);
          await updateUploadJobStatus(jobId, 'processing', Math.round(percent));
          await job.updateProgress(Math.round(percent));
          logger.info('Conversion progress', { percent: progress.percent });
        })
        .on('end', () => {
          logger.info('Video conversion completed');
          resolve();
        })
        .on('error', (err) => {
          logger.error('FFmpeg error', { error: err });
          reject(err);
        })
        .run();
    });

    await updateUploadJobStatus(jobId, 'processing', 85);
    await job.updateProgress(85);

    // Upload HLS files to S3
    logger.info('Uploading HLS files to S3');
    const hlsFiles = await fs.readdir(hlsOutputDir);
    const uploadPromises = hlsFiles.map(async (file) => {
      const filePath = path.join(hlsOutputDir, file);
      const s3Key = `videos/${contentId || episodeId}/${file}`;
      
      const fileContent = await fs.readFile(filePath);
      const contentType = file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T';
      
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: s3Key,
        Body: fileContent,
        ContentType: contentType,
        CacheControl: file.endsWith('.m3u8') ? 'no-cache' : 'public, max-age=31536000', // Cache segments for 1 year
      }));
      
      return s3Key;
    });

    await Promise.all(uploadPromises);
    
    // Get the playlist URL
    const playlistS3Key = `videos/${contentId || episodeId}/playlist.m3u8`;
    const playlistUrl = `${PUBLIC_URL}/${playlistS3Key}`;

    await updateUploadJobStatus(jobId, 'processing', 95);
    await job.updateProgress(95);

    // Update database with video URL
    if (episodeId) {
      await updateEpisodeVideo(episodeId, playlistUrl);
    } else if (contentId) {
      await updateContentVideo(contentId, playlistUrl);
    }

    // Delete original file from S3 (optional - keep for backup)
    // await s3Client.send(new DeleteObjectCommand({
    //   Bucket: BUCKET_NAME,
    //   Key: uploadPath.replace(`${PUBLIC_URL}/`, ''),
    // }));

    // Update job status to completed
    await updateUploadJobStatus(jobId, 'completed', 100, playlistUrl);
    await job.updateProgress(100);

    logger.info('Video processing completed successfully', {
      jobId,
      playlistUrl,
      hlsFileCount: hlsFiles.length,
    });

  } catch (error) {
    logger.error('Video processing failed', { error, jobId });
    await updateUploadJobStatus(jobId, 'failed', 0, undefined, error instanceof Error ? error.message : 'Unknown error');
    throw error;
  } finally {
    // Clean up temp files
    await fs.remove(tempDir);
  }
}

async function downloadFromS3(s3Url: string, localPath: string): Promise<void> {
  const s3Key = s3Url.replace(`${PUBLIC_URL}/`, '');
  
  const command = new GetObjectCommand({
    Bucket: BUCKET_NAME,
    Key: s3Key,
  });

  const response = await s3Client.send(command);
  
  if (!response.Body) {
    throw new Error('No body in S3 response');
  }

  const writeStream = fs.createWriteStream(localPath);
  
  return new Promise((resolve, reject) => {
    if (response.Body && 'pipe' in response.Body) {
      (response.Body as NodeJS.ReadableStream).pipe(writeStream)
        .on('error', reject)
        .on('close', resolve);
    } else {
      reject(new Error('Response body is not a readable stream'));
    }
  });
}