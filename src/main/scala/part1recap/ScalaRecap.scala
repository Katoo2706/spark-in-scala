package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  // val and vars
  val aBoolean: Boolean = true

  // expressions
  val anIfExpressions =
    if (2 > 3) "bigger"
    else "smaller"

  println(anIfExpressions)

  def myFunction(x: Int): Int = {
    x + x
  }
  print(myFunction(4))

  // anonymous funcion
  val myFunction2: Int => Int = x => x + x
  println(myFunction2(2))

  // generics
  trait MyList[A]

  // method notation
  // + actualy a method
  val x = 1 + 2
  val y = 1.+(1)

  println(y)

  // Functional Programming (Int => Int) = Function1[Int, Int]
  val incrementer: (Int => Int) = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementer2: (Int => Int) = x => x + 1

  println(List(1, 2, 3).map(incrementer))

  // Pattern matching
  val unknown: Any = 45
  val odinal = unknown match {
    case 1    => "first"
    case _: String  => "second"
    case exp: ExceptionInInitializerError => "Exception"
    case _    => "unkwon"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"Found $meaningOfLife")
    case Failure(exception) => println(s"Failed $exception")

  }

  // Partial function
  val aPartialFunction = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  val aPartitalFunction2: PartialFunction[Int, Int] = {
    case 1 => 43
    case 2 => 56
    case _ => 999
  }

  // Implicits

  // auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67

  println(methodWithImplicitArgument)

  val implicitCall = methodWithImplicitArgument

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  // fromStringToPerson("Bob").greet
  "Bob".greet

}