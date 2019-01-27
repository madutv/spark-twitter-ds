package org.abyas.twitter

import org.abyas.UnitTester

class TestTwitterFilter extends UnitTester {

  val filter1 = new TwitterFilters(
    olocations = Some("20;30, 30;50"),
    ofollow = Some("123, 343, 2223232323232323"),
    olanguages = Some("En, zd"),
    ocolumns = Some("a -> b -> c, z")
  )
  val filter2 = new TwitterFilters(ocolumns = Some("a -> b -> c, z"))

  test("Check if filter1 has language set to Array(En, zd)"){
    assert(filter1.languages.deep == Array("En", "zd").deep)
  }

  test("Check if location is set to Array[Array(20, 30), Array(30, 50))"){
    assert(filter1.locations.deep == Array(Array(20.0, 30.0), Array(30, 50)).deep)
  }

  test("Check if follow is set to Array(123, 343, 2223232323232323)"){
    assert(filter1.follow.deep == Array(123L, 343L,2223232323232323L).deep)
  }

  test("filterColumns is Array(Array(a, b, c), Array(z)"){
    val cols = Array(Array("a", "b", "c"), Array("z"))
    assert(filter1.filterColumns.deep == cols.deep)
  }

  test("track is empty Array"){
    assert(filter1.track.isEmpty)
  }

  test("Check if Twitter Filter Query is set"){
    assert(filter1.filterQuery.isDefined)
  }

  test("Check if Twitter Filter Query is set Empty"){
    assert(filter2.filterQuery.isEmpty)
  }

}
