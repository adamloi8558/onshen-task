import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { eq } from "drizzle-orm";

// Import schema from web app (assuming they share the same database)
interface UploadJob {
  id: string;
  job_id: string;
  user_id: string;
  content_id?: string;
  episode_id?: string;
  file_type: string;
  original_filename: string;
  file_size: number;
  upload_url?: string;
  processed_url?: string;
  status: string;
  progress: number;
  error_message?: string;
  created_at: Date;
  updated_at: Date;
}

interface User {
  id: string;
  avatar_url?: string;
}

interface Content {
  id: string;
  trailer_url?: string;
}

interface Episode {
  id: string;
  video_url?: string;
}

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL environment variable is required");
}

const client = postgres(process.env.DATABASE_URL, {
  max: 10,
  idle_timeout: 20,
  connect_timeout: 10,
});

export const db = drizzle(client, {
  logger: process.env.NODE_ENV === "development",
});

// Database operations for task worker
export async function updateUploadJobStatus(
  jobId: string, 
  status: string, 
  progress: number, 
  processedUrl?: string,
  errorMessage?: string
): Promise<void> {
  await db.execute(`
    UPDATE upload_jobs 
    SET status = $1, progress = $2, processed_url = $3, error_message = $4, updated_at = NOW()
    WHERE job_id = $5
  `, [status, progress, processedUrl || null, errorMessage || null, jobId]);
}

export async function updateUserAvatar(userId: string, avatarUrl: string): Promise<void> {
  await db.execute(`
    UPDATE users 
    SET avatar_url = $1, updated_at = NOW()
    WHERE id = $2
  `, [avatarUrl, userId]);
}

export async function updateContentVideo(contentId: string, videoUrl: string): Promise<void> {
  await db.execute(`
    UPDATE content 
    SET video_url = $1, updated_at = NOW()
    WHERE id = $2
  `, [videoUrl, contentId]);
}

export async function updateEpisodeVideo(episodeId: string, videoUrl: string): Promise<void> {
  await db.execute(`
    UPDATE episodes 
    SET video_url = $1, updated_at = NOW()
    WHERE id = $2
  `, [videoUrl, episodeId]);
}