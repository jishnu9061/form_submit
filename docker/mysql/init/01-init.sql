-- ============================================================
-- MySQL init script — runs once when the container is first created.
-- Creates the application database, testing database, and a
-- dedicated user with only the privileges the app needs.
-- ============================================================

-- Production database
CREATE DATABASE IF NOT EXISTS `app`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- Separate testing database so feature tests run against MySQL
-- (identical constraint behaviour to production, unlike SQLite).
CREATE DATABASE IF NOT EXISTS `app_testing`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- Dedicated application user (never use root in app code).
CREATE USER IF NOT EXISTS 'app_user'@'%' IDENTIFIED BY 'secret';

-- Grant minimum required privileges.
GRANT ALL PRIVILEGES ON `app`.*         TO 'app_user'@'%';
GRANT ALL PRIVILEGES ON `app_testing`.* TO 'app_user'@'%';

FLUSH PRIVILEGES;
