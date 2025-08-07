import { Job } from 'bullmq';
import sharp from 'sharp';
import fs from 'fs-extra';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { updateUploadJobStatus, db, client } from '../db';
import { logger } from '../utils/logger';
import { PosterUploadJob } from '../index';

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

export async function processPosterUpload(job: Job<PosterUploadJob['data']>): Promise<void> {
  const { jobId, userId, contentId, originalFilename, fileSize, uploadPath } = job.data;
  
  // Update job status to processing
  await updateUploadJobStatus(jobId, 'processing', 10);
  await job.updateProgress(10);

  const tempDir = path.join(process.cwd(), 'temp', jobId);
  
  try {
    // Create temp directory
    await fs.ensureDir(tempDir);

    // Download original file from S3
    logger.info('Downloading original poster file from S3', { uploadPath });
    const tempInputFile = path.join(tempDir, 'input.jpg');
    await downloadFromS3(uploadPath, tempInputFile);
    
    await updateUploadJobStatus(jobId, 'processing', 30);
    await job.updateProgress(30);

    // Process image with Sharp - create multiple sizes
    logger.info('Processing poster image');
    
    // Main poster (600x900)
    const posterBuffer = await sharp(tempInputFile)
      .resize(600, 900, {
        fit: 'cover',
        position: 'center'
      })
      .jpeg({
        quality: 90,
        progressive: true
      })
      .toBuffer();

    // Thumbnail (300x450)
    const thumbnailBuffer = await sharp(tempInputFile)
      .resize(300, 450, {
        fit: 'cover',
        position: 'center'
      })
      .jpeg({
        quality: 85,
        progressive: true
      })
      .toBuffer();

    await updateUploadJobStatus(jobId, 'processing', 70);
    await job.updateProgress(70);

    // Generate filenames
    const baseFilename = uuidv4();
    const posterS3Key = `posters/${contentId}/${baseFilename}.jpg`;
    const thumbnailS3Key = `posters/${contentId}/${baseFilename}_thumb.jpg`;

    // Upload both versions to S3
    logger.info('Uploading processed poster images to S3');
    
    await Promise.all([
      s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: posterS3Key,
        Body: posterBuffer,
        ContentType: 'image/jpeg',
        CacheControl: 'public, max-age=31536000',
      })),
      s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: thumbnailS3Key,
        Body: thumbnailBuffer,
        ContentType: 'image/jpeg',
        CacheControl: 'public, max-age=31536000',
      }))
    ]);

    const posterUrl = `${PUBLIC_URL}/${posterS3Key}`;
    const thumbnailUrl = `${PUBLIC_URL}/${thumbnailS3Key}`;

    await updateUploadJobStatus(jobId, 'processing', 90);
    await job.updateProgress(90);

    // Update content with poster URLs
    await client`
      UPDATE content 
      SET poster_url = ${posterUrl}, updated_at = NOW()
      WHERE id = ${contentId}
    `;

    // Delete original uploaded file
    try {
      const originalS3Key = uploadPath.replace(`${PUBLIC_URL}/`, '');
      await s3Client.send(new DeleteObjectCommand({
        Bucket: BUCKET_NAME,
        Key: originalS3Key,
      }));
    } catch (error) {
      logger.warn('Failed to delete original poster file', { error, uploadPath });
    }

    // Update job status to completed
    await updateUploadJobStatus(jobId, 'completed', 100, posterUrl);
    await job.updateProgress(100);

    logger.info('Poster processing completed successfully', {
      jobId,
      contentId,
      posterUrl,
      thumbnailUrl,
      originalSize: fileSize,
      posterSize: posterBuffer.length,
      thumbnailSize: thumbnailBuffer.length,
    });

  } catch (error) {
    logger.error('Poster processing failed', { error, jobId });
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