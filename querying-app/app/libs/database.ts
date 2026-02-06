import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";

const DB_PATH = path.join(process.cwd(), "home_assistant_blueprints.db");

let db: Awaited<ReturnType<typeof open>> | null = null;

export async function getDb() {
  if (!db) {
    db = await open({
      filename: DB_PATH,
      driver: sqlite3.Database,
    });
  }
  return db;
}