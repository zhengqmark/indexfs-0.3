// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "debugging.h"

#include <time.h>
#include <stdarg.h>

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

/* current logging level */
static log_level_t sys_log_level = DEFAULT_LOG_LEVEL;

/* core logging interface */
static void log_msg(const char *location, int log_level,
                    int newline_flag, const char *format, va_list ap);

/* append log only to stdout or stderr */
int giga_logopen(log_level_t level) {
  sys_log_level = level;
  return 0;
}

/* shutdown logging */
void giga_logclose(void) {
  fflush(stdout);
}

void logMessage(log_level_t level, const char *loc,
    const char *format, ...) {
  if (level > sys_log_level) return;

  va_list ap;
  va_start(ap, format);

  log_msg(loc, level, 1, format, ap);

  va_end(ap);
}

void logMessage_sameline(log_level_t level, const char *format, ...) {
  if (level > sys_log_level) return;

  va_list ap;
  va_start(ap, format);

  log_msg(NULL, level, 0, format, ap);

  va_end(ap);
}

static const char* log_level_str[] = {
    "FATAL", // 0
    "ERROR", // 1
    "WARN" , // 2
    "INFO" , // 3
    "DEBUG", // 4
    "TRACE"  // 5
};

static
void log_msg(const char *location, int log_level,
             int newline_flag, const char *format, va_list ap) {

  FILE* fp = log_level >= LOG_INFO ? stdout : stderr;

  char buffer[MAX_ERR_BUF_SIZE];

  vsnprintf(buffer, MAX_ERR_BUF_SIZE, format, ap);

  if (newline_flag != 0) {
    time_t t;
    t = time(NULL);
#if TIMESTAMP_ENABLED
    const char *TIMESTAMP_FMT = "%F %X"; /* = YYYY-MM-DD HH:MM:SS */
#define TS_BUF_SIZE sizeof("YYYY-MM-DD HH:MM:SS") /* This includes '\0' */
    struct tm *tm;
    char timestamp[TS_BUF_SIZE];

    tm = localtime(&t);
    strftime(timestamp, TS_BUF_SIZE, TIMESTAMP_FMT, tm);
    fprintf(fp, "%s", timestamp);
#else
    fprintf(fp, "%lu", t);
#endif

    if (location != NULL) fprintf(fp, " <%s>", location);

    fprintf(fp, " [%s] - ", log_level_str[log_level]);

    strcat(buffer, "\n");
  }

  fprintf(fp, "%s", buffer);

  if (log_level >= LOG_INFO && newline_flag != 0) fflush(fp);
}
