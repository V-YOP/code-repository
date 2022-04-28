import scala.annotation.tailrec

case class RNG(seed: Long) {
  def nextInt: (Int, RNG) = {
    val newSeed = (seed * 0x5DEECE66DL + 0xBL) & 0xFFFFFFFFFFFFL
    val nextRNG = RNG(newSeed)
    val n = (newSeed >>> 16).toInt
    (n, nextRNG)
  }
}

case object RNG {
  @tailrec
  def nonNegativeInt(rng: RNG): (Int, RNG) = {
    val (v, r) = rng.nextInt
    if (v == Int.MinValue) nonNegativeInt(r)
    else (Math.abs(v), r)
  }

  def double(rng: RNG): (Double, RNG) = {
    val (v, r) = nonNegativeInt(rng)
    (v / Int.MaxValue.toDouble, r)
  }

  def intDouble(rng: RNG): ((Int, Double), RNG) = {
    val (v1, r1) = rng.nextInt
    val (v2, r2) = double(r1)
    ((v1, v2), r2)
  }

  def doubleInt(rng: RNG): ((Double, Int), RNG) = {
    val (v1, r1) = double(rng)
    val (v2, r2) = r1.nextInt
    ((v1, v2), r2)
  }

  def double3(rng: RNG): ((Double, Double, Double), RNG) = {
    val (v1, r1) = double(rng)
    val (v2, r2) = double(r1)
    val (v3, r3) = double(r2)
    ((v1, v2, v3), r3)
  }

  def ints(count: Int)(rng: RNG): (List[Int], RNG) =
    if (count == 0) (Nil, rng)
    else {
      val (v, r) = rng.nextInt
      val (last, s) = ints(count - 1)(r)
      (v :: last, s)
    }

  // 题目 6.4 及其后
  type Rand[+A] = RNG => (A, RNG)

  def unit[A](a: A): Rand[A] = rng => (a, rng)

  def map[A, B](a: Rand[A])(f: A => B): Rand[B] = rng => {
    val (v, r) = a(rng)
    (f(v), r)
  }

  val double$: Rand[Double] = map(nonNegativeInt)(_ / Int.MaxValue.toDouble)

  def map2[A, B, C](a: Rand[A], b: Rand[B])(f: (A, B) => C): Rand[C] = rng => {
    val (v1, r1) = a(rng)
    val (v2, r2) = b(r1)
    (f(v1, v2), r2)
  }

  def sequence[A](fs: List[Rand[A]]): Rand[List[A]] = rng => fs match {
    case Nil => (Nil, rng)
    case x :: xs => map2(x, sequence(xs))(_ :: _)(rng)
  }

  def ints$(count: Int): Rand[List[Int]] =
    sequence(List.fill(count)(_.nextInt))

  def flatMap[A, B](a: Rand[A])(f: A => Rand[B]): Rand[B] = rng => {
    val (v, r) = a(rng)
    f(v)(r)
  }

  def nonNegativeLessThan(n: Int): Rand[Int] =
    flatMap(nonNegativeInt) { i =>
      val mod = i % n
      if (i + (n - 1) - mod >= 0) unit(mod)
      else nonNegativeLessThan(n)
    }

  // 编写的时候应当将flatMap理解为顺序计算，这里先计算了a，将a的结果拿出来进行进一步使用
  // 这样理解大概对一切monad都适用——对某monad进行"计算"，将结果临时取出来进行进一步操作
  def map$[A, B](a: Rand[A])(f: A => B): Rand[B] = flatMap(a) { i => unit(f(i)) }

  def map2$[A, B, C](ra: Rand[A], rb: Rand[B])(f: (A, B) => C): Rand[C] =
    flatMap(ra)(a => map(rb)(b => f(a, b))) // 或者 flatMap(ra)(a => flatMap(rb)(b => unit(f(a, b))))
}

final case class State[S, +A](run: S => (A, S)) {
  def apply(s: S): (A, S) = run(s)

  // map方法就是构造一个新的State，该State为当前State执行后对结果进行一个映射
  def map[B](f: A => B): State[S, B] = State { s =>
    val (r, nextS) = run(s)
    (f(r), nextS)
  }

  def map2[B, C](b: State[S, B])(f: (A, B) => C): State[S, C] = State { s =>
    val (r1, s1) = run(s)
    val (r2, s2) = b.run(s1)
    (f(r1, r2), s2)
  }

  // 先执行当前计算，将下一个值传给f，对生成的State执行计算
  def flatMap[B](f: A => State[S, B]): State[S, B] = State { s =>
    val (r1, s1) = run(s)
    f(r1).run(s1)
  }
}

case object State {
  def unit[S, A](a: A): State[S, A] = State((a, _))

  def sequence[S, A](a: List[State[S, A]]): State[S, List[A]] = a match {
    case Nil => State((Nil, _))
    case x :: xs => x.map2(sequence(xs))(_ :: _)
  }

  def get[S]: State[S, S] = State(s => (s, s))

  def set[S](s: S): State[S, Unit] = State(_ => ((), s))

  def modify[S](f: S => S): State[S, Unit] = for {
    s <- get
    _ <- set(f(s))
  } yield ()
}

// 虽然书中没有要求，但是重新实现它们是非常有趣的，也是为了实践经验
case object StateRNG {
  type Rand[+A] = State[RNG, A]
  val int: Rand[Int] = State(_.nextInt)
  @tailrec
  val nonNegativeInt: Rand[Int] = int.flatMap { i =>
    if (i != Int.MinValue) State.unit(i).map(Math.abs)
    else nonNegativeInt
  }
  val double: Rand[Double] = nonNegativeInt.map(_ / Int.MaxValue.toDouble)
}


sealed trait Input

case object Coin extends Input

case object Turn extends Input

case class Machine(locked: Boolean, coins: Int, candies: Int)

case object Machine {
  def operate(input: Input): State[Machine, (Int, Int)] = State { state =>
    def deconstruct(state: Machine): ((Int, Int), Machine) = ((state.coins, state.candies), state)
    // 有三个参数：Input，locked，candies <= 0?
    (input, state.locked, state.candies) match {
      case (input, _, candies) if candies <= 0 => input match {
        case Coin => deconstruct(state.copy(coins = state.coins + 1))
        case Turn => deconstruct(state)
      }
      case (Coin, _, _) => deconstruct(state.copy(locked = false, coins = state.coins + 1))
      case (Turn, false, candies) => deconstruct(state.copy(locked = true, candies = candies - 1))
      case (Turn, true, _) => deconstruct(state)
    }
  }

  def simulateMachine(input: List[Input]): State[Machine, (Int, Int)] = input match {
    case Nil => State.get[Machine].map(s => (s.coins, s.candies))
    case x :: xs => operate(x).flatMap(_ => simulateMachine(xs))
  }
}
