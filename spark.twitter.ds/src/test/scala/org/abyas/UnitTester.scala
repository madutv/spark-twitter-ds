package org.abyas

import org.scalatest._

abstract class UnitTester extends FunSuite with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfter
