package ca.mcit.bigdata.hadoop

trait Join[L,R,Q] {

  def join(a: List[L], b: List[R]): List[Q]

}

case class JoinOutput(left: Any, right: Option[Any])

// Generic Map Join
class GenericMapJoin[L,R] (val joinCond: (L) => String ) (val joinCond1: (R) => String ) extends Join[L,R, JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = {

    val l: Map[String, R] = b
      .map(b => joinCond1(b) -> b)
      .toMap

    a
      .filter(a => l.contains(joinCond(a)))
      .map(a => JoinOutput(a, Some(l(joinCond(a)))))
  }
}

// Generic Nested Loop
class GenericNestedLoop[L,R] (val joinCond: (L,R) => Boolean ) extends Join[L,R,JoinOutput] {
  override def join(a: List[L], b: List[R]): List[JoinOutput] = for {
    l <- a
    r <- b

    if joinCond(l, r)
  } yield JoinOutput(l, Some(r))
}

// Practise for Higher Order function
/*class GenericMapJoin1[L,R] extends Join[L,R, JoinOutput, JoinCond] {

  override def join(a: List[L], b: List[R]) (i: JoinCond) : List[JoinOutput] = {

    val l: Map[String, R] = b
      .map(b => JoinCond(b).toString -> b)
      .toMap

    a
      .filter(a => l.contains(JoinCond(a).toString))
      .map(a => JoinOutput(a, Some(l(JoinCond(a).toString))))
  }
}*/

