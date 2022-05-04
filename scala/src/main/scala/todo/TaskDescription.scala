package todo

case class TaskDescription(
                          rawDescription: String,
                          projectTags: Seq[String],
                          contextTags: Seq[String],
                          kvTags: Map[String, String],
                          ) {
  override def toString: String = rawDescription
}
case object TaskDescription {
  import scala.language.implicitConversions
  implicit def stringToTaskDescription(rawStr: String): TaskDescription = {
    // TODO
    TaskDescription(rawStr, Seq(),Seq(),Map())
  }
}