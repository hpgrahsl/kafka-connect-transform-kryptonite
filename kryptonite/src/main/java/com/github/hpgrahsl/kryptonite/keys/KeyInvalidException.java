/*
 * Copyright (c) 2021. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.hpgrahsl.kryptonite.keys;

@SuppressWarnings("serial")
public class KeyInvalidException  extends KeyException {

  public KeyInvalidException() {
  }

  public KeyInvalidException(String message) {
    super(message);
  }

  public KeyInvalidException(String message, Throwable cause) {
    super(message, cause);
  }

  public KeyInvalidException(Throwable cause) {
    super(cause);
  }

  public KeyInvalidException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
