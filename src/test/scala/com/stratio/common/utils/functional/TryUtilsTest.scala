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

import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.util.{Failure, Success}

class TryUtilsTest extends FlatSpec with Matchers with Inside{

  val exception = new Exception("Error")

  behavior of "Try"

  it should "allow transforming a Seq[Try[A]] containing a Failure into a Failure" in {

    TryUtils.sequence(Seq(Success(1), Failure(exception))) shouldBe a[Failure[_]]

  }

  it should "allow transforming a Seq[Try[A]] without failures into a Success[Seq[A]]" in {

    inside(TryUtils.sequence(Seq(Success(1), Success(2)))) {
      case Success(seq) => seq should contain theSameElementsInOrderAs Seq(1,2)
    }

  }

  it should "allow transforming a Seq[Try[A]] of 100000 values into a Success[Seq[A]]" in {

    val seqSuccess = (1 to 100000) map (Success(_))
    noException shouldBe thrownBy (TryUtils.sequence(seqSuccess))

  }


}