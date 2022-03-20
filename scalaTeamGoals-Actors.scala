// Scala and Akka Actors Import
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scala.collection.mutable.HashMap
import scala.io.Source
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.Await

// Main Actor
class charCountActor() extends Actor{
  var result = HashMap[Char,Int]();
  var sendResultTo : ActorRef = context.self
  // Receive Function for reading message input and processing
  def receive:Receive = {
    // If received message is List[String]
    case wordList:List[String] => {
      // Storing Context of Main function for sending final output
      sendResultTo = sender()
      // Message sent to child actor for processing
      val countActor = context.actorOf(Props[stringProcessActor])
      for (word <- wordList){
        countActor ! word.toList
      }
    }
    // Data is received as Hashmap from Child Actor
    case processedWord:HashMap[Char,Int] => {
      for(chars <- processedWord) {
        if(result.contains(chars._1) != false) {
          result.put(chars._1,result(chars._1) + chars._2);
        } else {
          result.put(chars._1,chars._2);
        }
      }
      // Sending final result to Main Function
      sendResultTo ! processedWord
    }
  }
}

// Child Actor for processing the message received from Parent Actor
class stringProcessActor() extends Actor {
  var hashMapCharCount = HashMap[Char,Int]()
  // Receive Method for Child Actor
  def receive:Receive = {
    case charList:List[Char] => {
      for (chars <- charList){
        if(hashMapCharCount.contains(chars) != false) {
          hashMapCharCount.put(chars,hashMapCharCount(chars) + 1);
        } else {
          hashMapCharCount.put(chars,1);
        }
      }
      // Sending Data back to Parent Actor
      sender() ! hashMapCharCount
    }
  }
}

// Main Function
object scalaTeamGoals extends App {
    // Reading Data from input file
    val sourceFile = Source.fromFile("src/main/scala/input.txt")
    val inputLines = try sourceFile.getLines().mkString finally sourceFile.close()
    // Defining main actor
    val system = ActorSystem("charCount");
    val mainActor = system.actorOf(Props[charCountActor],name="charCount")
    // Timeout used to wait output for 10sec
    val timeout = Timeout (FiniteDuration(Duration("10 seconds").toSeconds, SECONDS))
    // Ask function sends the message and returns a Future
    // future is a object for a result that is not yet known.
    val future = ask (mainActor,inputLines.split(" ").toList) (timeout)
    val result = Await.result (future, timeout.duration)
    // Printing final output
    println ("The sorted list is " + result)
    // Terminating the Actor system
    system.terminate();
}
