package todo

private case object NoDate extends TaskDate
private case class BothDate(completedDate: String, createdDate: String) extends TaskDate
private case class OnlyCreatedDate(createdDate: String) extends TaskDate

sealed trait TaskDate {
  override def toString: String = this match {
    case NoDate => ""
    case BothDate(completedDate, createdDate) => completedDate + " " + createdDate
    case OnlyCreatedDate(createdDate) => createdDate
  }
}

object TaskDate {
  def apply(): TaskDate = NoDate
  def apply(createdDate: String): TaskDate = OnlyCreatedDate(createdDate)
  def apply(completedDate: String, createdDate: String):TaskDate = BothDate(completedDate, createdDate)
}