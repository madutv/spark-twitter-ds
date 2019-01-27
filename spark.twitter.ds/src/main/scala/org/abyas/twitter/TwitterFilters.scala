package org.abyas.twitter

import com.typesafe.scalalogging.Logger

import twitter4j.FilterQuery

import org.abyas.twitter.TwitterConsts.{PRIMARY_DELIMITER, SECONDARY_DELIMITER, PATH_DELIMITER}
import org.abyas.utils.implicits.OptionImplicits.OptionStringImplicit
import org.abyas.utils.implicits.StringImplicits.StringImplicit
import org.abyas.utils.implicits.TryImplicits.TryAndDefaultImplicityHelpers


/**
  * Class to Create TwitterFilter as well as set columns that need to be
  * extracted from Twitter Stream
  *
  * @param ofollow: Comma separated list of Long User IDs. Stream will
  *               be filtered only for tweets or re-tweets from this user.
  *               See Twitter API docs for further details.
  *
  * @param otrack: Comma separated list of tags. Stream will be filtered
  *              only for tweets with these strings. See Twitter API docs
  *
  * @param olocations: Co-ordinates to filter. This should be a String
  *                  like so: 30;40, 50;70.
  *
  * @param olanguages: Comma separated String of Languages
  *
  * @param ocolumns: Columns to extract from Stream. If the column of
  *                interest is nested, then the path of extraction should
  *                be specified. For example: If text and url needs to be
  *                extracted, then this field will be: "text, entities -> urls -> url"
  *                This is because "text" is at root level, however url is
  *                nested under entities -> urls
  *
  */
class TwitterFilters(ofollow: Option[String] = None,
                     otrack: Option[String] = None,
                     olocations: Option[String] = None,
                     olanguages: Option[String] = None,
                     ocolumns: Option[String] = None) {

  val logger = Logger("TwitterFilters")

  val follow: Array[Long] = ofollow.toLongArray(PRIMARY_DELIMITER)
  val track: Array[String] = otrack.toStringArray(PRIMARY_DELIMITER)
  val locations: Array[Array[Double]] = olocations
                                        .toStringArray(PRIMARY_DELIMITER)
                                        .map(a => a.toDoubleArray(SECONDARY_DELIMITER))
  val languages: Array[String] = olanguages.toStringArray(PRIMARY_DELIMITER)
  private var columns: Array[String] = ocolumns.toStringArray(PRIMARY_DELIMITER)

  val filterQuery: Option[FilterQuery] = createTwitterFilter()
  val filterColumns: Array[Array[String]] = createTwitterFilterColumns()


  /**
    * This method checks if any of follow, track, locations, lanaguages are
    * provided, if so, then a Twitter's FilterQuery will be created based
    * on them. Else None will be returned
    *
    * @return FilterQuery or None
    */
  def createTwitterFilter(): Option[FilterQuery] = {
    if (follow.isEmpty && track.isEmpty && locations.isEmpty && languages.isEmpty)
      None
    else
      Some(new FilterQuery(0, follow, track, locations, languages))
  }

  /**
    * This method converts columns from Map[String, String] to
    * Map[String, Array[String]], where the Array[String] is the depth from where
    * to extract Twitter contents. For example, to get expanded_urls we'd have
    * Array["entities", "urls", "expanded_url"]
    *
    * @return
    */
  def createTwitterFilterColumns(): Array[Array[String]] = {
    columns match {
      case a if a.isEmpty => Array()
      case a => a.map(v => v.toStringArray(PATH_DELIMITER))
    }
  }

}
