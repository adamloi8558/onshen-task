import dotenv from 'dotenv';
import 'dotenv/config';
import { Worker, Job } from 'bullmq';
import Redis from 'ioredis';
import { processVideoUpload, processAvatarUpload, processPosterUpload, processYoutubeDownload } from './processors';
import { logger } from './utils/logger';

// Load environment variables
dotenv.config();

const redis = new Redis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null, // Required for BullMQ
  lazyConnect: true,
});

// Job types
export interface VideoUploadJob {
  type: 'video_upload';
  data: {
    jobId: string;
    userId: string;
    contentId?: string;
    episodeId?: string;
    originalFilename: string;
    fileSize: number;
    uploadPath: string; // S3 path to original file
  };
}

export interface AvatarUploadJob {
  type: 'avatar_upload';
  data: {
    jobId: string;
    userId: string;
    originalFilename: string;
    fileSize: number;
    uploadPath: string; // S3 path to original file
    oldAvatarPath?: string; // Previous avatar to delete
  };
}

export interface PosterUploadJob {
  type: 'poster_upload';
  data: {
    jobId: string;
    userId: string;
    contentId: string;
    originalFilename: string;
    fileSize: number;
    uploadPath: string; // S3 path to original file
  };
}

export type JobData = VideoUploadJob | AvatarUploadJob | PosterUploadJob;

// Create workers for different job types
const videoWorker = new Worker(
  'video-processing',
  async (job: Job<any>) => {
    logger.info(`Processing job ${job.id}`, { name: job.name, jobData: job.data });
    try {
      if (job.name === 'download-youtube') {
        // YouTube download job
        await processYoutubeDownload(job);
      } else if (job.name === 'process-video') {
        // Regular video upload job
        await processVideoUpload(job);
      } else {
        logger.error(`Unknown job type: ${job.name}`);
        throw new Error(`Unknown job type: ${job.name}`);
      }
    } catch (error) {
      logger.error(`Job ${job.id} failed`, { error, name: job.name, jobData: job.data });
      throw error;
    }
  },
  {
    connection: redis,
    concurrency: 2, // Process 2 videos at once
  }
);

const imageWorker = new Worker(
  'image-processing',
  async (job: Job<AvatarUploadJob['data'] | PosterUploadJob['data']>) => {
    logger.info(`Processing image job ${job.id}`, { jobData: job.data });
    try {
      const jobData = job.data as any;
      if (jobData.contentId) {
        // Poster upload
        await processPosterUpload(job as Job<PosterUploadJob['data']>);
      } else {
        // Avatar upload
        await processAvatarUpload(job as Job<AvatarUploadJob['data']>);
      }
    } catch (error) {
      logger.error(`Image job ${job.id} failed`, { error, jobData: job.data });
      throw error;
    }
  },
  {
    connection: redis,
    concurrency: 5, // Process 5 images at once
  }
);

// Worker event handlers
videoWorker.on('completed', (job) => {
  logger.info(`Video job ${job.id} completed successfully`);
});

videoWorker.on('failed', (job, err) => {
  logger.error(`Video job ${job?.id} failed`, { error: err });
});

imageWorker.on('completed', (job) => {
  logger.info(`Image job ${job.id} completed successfully`);
});

imageWorker.on('failed', (job, err) => {
  logger.error(`Image job ${job?.id} failed`, { error: err });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('Received SIGTERM, shutting down gracefully...');
  await videoWorker.close();
  await imageWorker.close();
  await redis.quit();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('Received SIGINT, shutting down gracefully...');
  await videoWorker.close();
  await imageWorker.close();
  await redis.quit();
  process.exit(0);
});

logger.info('Task workers started successfully', {
  videoWorkerConcurrency: 2,
  imageWorkerConcurrency: 5,
  redisUrl: process.env.REDIS_URL,
});

export { videoWorker, imageWorker };