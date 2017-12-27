/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "netty_unix_util.h"

#ifdef NETTY_USE_MACH_INSTEAD_OF_CLOCK

#include <mach/mach.h>
#include <mach/mach_time.h>
static const uint64_t NETTY_BILLION = 1000000000L;

#endif /* NETTY_USE_MACH_INSTEAD_OF_CLOCK */

char* netty_unix_util_prepend(const char* prefix, const char* str) {
    if (prefix == NULL) {
        char* result = (char*) malloc(sizeof(char) * (strlen(str) + 1));
        strcpy(result, str);
        return result;
    }
    char* result = (char*) malloc(sizeof(char) * (strlen(prefix) + strlen(str) + 1));
    strcpy(result, prefix);
    strcat(result, str);
    return result;
}

char* netty_unix_util_rstrstr(char* s1rbegin, const char* s1rend, const char* s2) {
    size_t s2len = strlen(s2);
    char *s = s1rbegin - s2len;

    for (; s >= s1rend; --s) {
        if (strncmp(s, s2, s2len) == 0) {
            return s;
        }
    }
    return NULL;
}

static char* netty_unix_util_strstr_last(const char* haystack, const char* needle) {
    char* prevptr = NULL;
    char* ptr = (char*) haystack;

    while ((ptr = strstr(ptr, needle)) != NULL) {
        // Just store the ptr and continue searching.
        prevptr = ptr;
        ++ptr;
    }
    return prevptr;
}

char* netty_unix_util_parse_package_prefix(const char* libraryPathName, const char* libraryName, jint* status) {
    char* packageNameEnd = netty_unix_util_strstr_last(libraryPathName, libraryName);
    if (packageNameEnd == NULL) {
        *status = JNI_ERR;
        return NULL;
    }
    char* packagePrefix = netty_unix_util_rstrstr(packageNameEnd, libraryPathName, "lib");
    if (packagePrefix == NULL) {
        *status = JNI_ERR;
        return NULL;
    }
    packagePrefix += 3;
    if (packagePrefix == packageNameEnd) {
        return NULL;
    }
    // packagePrefix length is > 0
    // Make a copy so we can modify the value without impacting libraryPathName.
    size_t packagePrefixLen = packageNameEnd - packagePrefix;
    packagePrefix = strndup(packagePrefix, packagePrefixLen);
    // Make sure the packagePrefix is in the correct format for the JNI functions it will be used with.
    char* temp = packagePrefix;
    packageNameEnd = packagePrefix + packagePrefixLen;
    // Package names must be sanitized, in JNI packages names are separated by '/' characters.
    for (; temp != packageNameEnd; ++temp) {
        if (*temp == '_') {
            *temp = '/';
        }
    }
    // Make sure packagePrefix is terminated with the '/' JNI package separator.
    if(*(--temp) != '/') {
        temp = packagePrefix;
        packagePrefix = netty_unix_util_prepend(packagePrefix, "/");
        free(temp);
    }
    return packagePrefix;
}

// util methods
int netty_unix_util_clock_gettime(clockid_t clockId, struct timespec* tp) {
#ifdef NETTY_USE_MACH_INSTEAD_OF_CLOCK
  uint64_t timeNs;
  switch (clockId) {
  case CLOCK_MONOTONIC_COARSE:
    timeNs = mach_approximate_time();
    break;
  case CLOCK_MONOTONIC:
    timeNs = mach_absolute_time();
    break;
  default:
    errno = EINVAL;
    return -1;
  }
  // NOTE: this could overflow if time_t is backed by a 32 bit number.
  tp->tv_sec = timeNs / NETTY_BILLION;
  tp->tv_nsec = timeNs - tp->tv_sec * NETTY_BILLION; // avoid using modulo if not necessary
  return 0;
#else
  return clock_gettime(clockId, tp);
#endif /* NETTY_USE_MACH_INSTEAD_OF_CLOCK */
}

jboolean netty_unix_util_initialize_wait_clock(clockid_t* clockId) {
  struct timespec ts;
  // First try to get a monotonic clock, as we effectively measure execution time and don't want the underlying clock
  // moving unexpectedly/abruptly.
#ifdef CLOCK_MONOTONIC_COARSE
  *clockId = CLOCK_MONOTONIC_COARSE;
  if (netty_unix_util_clock_gettime(*clockId, &ts) == 0) {
    return JNI_TRUE;
  }
#endif
#ifdef CLOCK_MONOTONIC_RAW
  *clockId = CLOCK_MONOTONIC_RAW;
  if (netty_unix_util_clock_gettime(*clockId, &ts) == 0) {
    return JNI_TRUE;
  }
#endif
#ifdef CLOCK_MONOTONIC
  *clockId = CLOCK_MONOTONIC;
  if (netty_unix_util_clock_gettime(*clockId, &ts) == 0) {
    return JNI_TRUE;
  }
#endif

  // Fallback to realtime ... in this case we are subject to clock movements on the system.
#ifdef CLOCK_REALTIME_COARSE
  *clockId = CLOCK_REALTIME_COARSE;
  if (netty_unix_util_clock_gettime(*clockId, &ts) == 0) {
    return JNI_TRUE;
  }
#endif
#ifdef CLOCK_REALTIME
  *clockId = CLOCK_REALTIME;
  if (netty_unix_util_clock_gettime(*clockId, &ts) == 0) {
    return JNI_TRUE;
  }
#endif

  return JNI_FALSE;
}

jint netty_unix_util_register_natives(JNIEnv* env, const char* packagePrefix, const char* className, const JNINativeMethod* methods, jint numMethods) {
    char* nettyClassName = netty_unix_util_prepend(packagePrefix, className);
    jclass nativeCls = (*env)->FindClass(env, nettyClassName);
    free(nettyClassName);
    nettyClassName = NULL;
    if (nativeCls == NULL) {
        return JNI_ERR;
    }

    return (*env)->RegisterNatives(env, nativeCls, methods, numMethods);
}
