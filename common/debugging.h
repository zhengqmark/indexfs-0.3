// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_LEGACY_DEBUGGING_H_
#define _INDEXFS_LEGACY_DEBUGGING_H_

/* 
 * Logging related definitions.
 */
typedef enum log_level {
  LOG_FATAL, LOG_ERR, LOG_WARN, LOG_INFO, LOG_DEBUG, LOG_TRACE
} log_level_t;

#define DEFAULT_LOG_LEVEL         LOG_DEBUG
#define TIMESTAMP_ENABLED         0
#define MAX_ERR_BUF_SIZE          1024

/*
 * Basic Logging Interface.
 */
#define INDEXFS_FATAL( format, ...)  logMessage(LOG_ERR  , __func__, format, __VA_ARGS__)
#define INDEXFS_ERR(   format, ...)  logMessage(LOG_ERR  , __func__, format, __VA_ARGS__)
#define INDEXFS_WARN(  format, ...)  logMessage(LOG_WARN , __func__, format, __VA_ARGS__)
#define INDEXFS_INFO(  format, ...)  logMessage(LOG_INFO , __func__, format, __VA_ARGS__)
#define INDEXFS_DEBUG( format, ...)  logMessage(LOG_DEBUG, __func__, format, __VA_ARGS__)

/*
 * Core Logging Interface.
 */
int giga_logopen(log_level_t level);
void giga_logclose(void);

void logMessage(log_level_t level, const char *location, const char *format, ...);
void logMessage_sameline(log_level_t level, const char *format, ...);

/* deprecated interface - not recommended to use! */
#define LOG_ERR(format, ...) logMessage(LOG_ERR, __func__, format, __VA_ARGS__)

/*
 * Macros for mutex debugging.
 */
#define ACQUIRE_MUTEX(lock, format, ...)                             \
{                                                                    \
  logMessage(LOG_DEBUG, "LOCK_TRY", format, __VA_ARGS__);            \
  pthread_mutex_lock(lock);                                          \
  logMessage(LOG_DEBUG, "LOCK_DONE", format, __VA_ARGS__);           \
}
#define RELEASE_MUTEX(lock, format, ...)                             \
{                                                                    \
  logMessage(LOG_DEBUG, "UNLOCK_TRY", format, __VA_ARGS__);          \
  pthread_mutex_unlock(lock);                                        \
  logMessage(LOG_DEBUG, "UNLOCK_DONE", format, __VA_ARGS__);         \
}
#define ACQUIRE_RWLOCK_READ(lock, format, ...)                       \
{                                                                    \
  logMessage(LOG_DEBUG, "LOCK_RD_TRY", format, __VA_ARGS__);         \
  pthread_rwlock_rdlock(lock);                                       \
  logMessage(LOG_DEBUG, "LOCK_RD_DONE", format, __VA_ARGS__);        \
}
#define ACQUIRE_RWLOCK_WRITE(lock, format, ...)                      \
{                                                                    \
  logMessage(LOG_DEBUG, "LOCK_WR_TRY", format, __VA_ARGS__);         \
  pthread_rwlock_wrlock(lock);                                       \
  logMessage(LOG_DEBUG, "LOCK_WR_DONE", format, __VA_ARGS__);        \
}
#define RELEASE_RWLOCK(lock, format, ...)                            \
{                                                                    \
  logMessage(LOG_DEBUG, "UNLOCK_RW_TRY", format, __VA_ARGS__);       \
  pthread_rwlock_unlock(lock);                                       \
  logMessage(LOG_DEBUG, "UNLOCK_RW_DONE", format, __VA_ARGS__);      \
}

#endif /* _INDEXFS_LEGACY_DEBUGGING_H_ */
