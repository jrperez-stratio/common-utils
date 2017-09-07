/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.common.utils.functional

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object TryUtils {

  def sequence[T](s: Seq[Try[T]]): Try[Seq[T]] = {

    @tailrec
    def recSequence(s: Seq[Try[T]], acc: Seq[T]): Try[Seq[T]] =
      if (s.nonEmpty) {
        s.head match {
          case Success(v) => recSequence(s.tail, v +: acc)
          case Failure(cause) => Failure(cause)
        }
      } else {
        Success(acc reverse)
      }
    recSequence(s, Seq.empty)
  }

}
