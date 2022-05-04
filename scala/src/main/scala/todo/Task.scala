package todo

import scala.util.Try

class Task private(
                         val description: TaskDescription,
                         val done: Boolean = false,
                         val priority: Option[Char] = None,
                         val date: TaskDate = TaskDate()
                       ) {
  override def toString: String = List(
    if (done) "x" else "",
    priority.map(c => f"($c)").getOrElse(""),
    date.toString,
    description.toString
  ).filter(_.nonEmpty).mkString(" ")
}

case object Task {
  // TODO 这个重头，得参考shell中的处理方式，另外开文件写
  def parse(taskStr: String): Try[Task] = Try {
    new Task("hello!")
  }
}