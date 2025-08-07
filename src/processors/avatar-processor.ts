import { Job } from 'bullmq';
import sharp from 'sharp';
import fs from 'fs-extra';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { updateUploadJobStatus, updateUserAvatar } from '../db';
import { logger } from '../utils/logger';
import { AvatarUploadJob } from '../index';

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

export async function processAvatarUpload(job: Job<AvatarUploadJob['data']>): Promise<void> {
  const { jobId, userId, originalFilename, fileSize, uploadPath, oldAvatarPath } = job.data;
  
  // Update job status to processing
  await updateUploadJobStatus(jobId, 'processing', 10);
  await job.updateProgress(10);

  const tempDir = path.join(process.cwd(), 'temp', jobId);
  
  try {
    // Create temp directory
    await fs.ensureDir(tempDir);

    // Download original file from S3
    logger.info('Downloading original avatar file from S3', { uploadPath });
    const tempInputFile = path.join(tempDir, 'input.jpg');
    await downloadFromS3(uploadPath, tempInputFile);
    
    await updateUploadJobStatus(jobId, 'processing', 30);
    await job.updateProgress(30);

    // Process image with Sharp
    logger.info('Processing avatar image');
    const processedImageBuffer = await sharp(tempInputFile)
      .resize(500, 500, {
        fit: 'cover',
        position: 'center'
      })
      .jpeg({
        quality: 90,
        progressive: true
      })
      .toBuffer();

    await updateUploadJobStatus(jobId, 'processing', 70);
    await job.updateProgress(70);

    // Generate new filename
    const fileExtension = '.jpg';
    const newFilename = `${uuidv4()}${fileExtension}`;
    const s3Key = `avatars/${newFilename}`;

    // Upload processed image to S3
    logger.info('Uploading processed avatar to S3', { s3Key });
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: s3Key,
      Body: processedImageBuffer,
      ContentType: 'image/jpeg',
      CacheControl: 'public, max-age=31536000', // Cache for 1 year
    }));

    const avatarUrl = `${PUBLIC_URL}/${s3Key}`;

    await updateUploadJobStatus(jobId, 'processing', 90);
    await job.updateProgress(90);

    // Update user avatar in database
    await updateUserAvatar(userId, avatarUrl);

    // Delete old avatar if exists (but not default avatar)
    if (oldAvatarPath && !oldAvatarPath.includes('default.webp')) {
      try {
        const oldS3Key = oldAvatarPath.replace(`${PUBLIC_URL}/`, '');
        await s3Client.send(new DeleteObjectCommand({
          Bucket: BUCKET_NAME,
          Key: oldS3Key,
        }));
        logger.info('Old avatar deleted', { oldS3Key });
      } catch (error) {
        logger.warn('Failed to delete old avatar', { error, oldAvatarPath });
        // Don't fail the job if old avatar deletion fails
      }
    }

    // Delete original uploaded file
    try {
      const originalS3Key = uploadPath.replace(`${PUBLIC_URL}/`, '');
      await s3Client.send(new DeleteObjectCommand({
        Bucket: BUCKET_NAME,
        Key: originalS3Key,
      }));
    } catch (error) {
      logger.warn('Failed to delete original file', { error, uploadPath });
    }

    // Update job status to completed
    await updateUploadJobStatus(jobId, 'completed', 100, avatarUrl);
    await job.updateProgress(100);

    logger.info('Avatar processing completed successfully', {
      jobId,
      userId,
      avatarUrl,
      originalSize: fileSize,
      processedSize: processedImageBuffer.length,
    });

  } catch (error) {
    logger.error('Avatar processing failed', { error, jobId });
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