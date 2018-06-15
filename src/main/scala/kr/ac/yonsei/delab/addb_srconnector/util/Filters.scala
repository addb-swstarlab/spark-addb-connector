package kr.ac.yonsei.delab.addb_srconnector.util

import scala.collection.mutable.Stack
import org.apache.spark.sql.sources._

object Filters { 
    def makeFilterString(f: Filter, stack: Stack[String]) : Unit = {
    f match {
      case Or(_,_) =>  { 
        stack.push(":Or")
        makeFilterString(f.asInstanceOf[Or].left, stack)
        makeFilterString(f.asInstanceOf[Or].right, stack)
      }
      case And(_,_) =>  {
        stack.push(":And")
        makeFilterString(f.asInstanceOf[And].left, stack)
        makeFilterString(f.asInstanceOf[And].right, stack)
      }
      
      case EqualTo(_,_) => {
        stack.push(":EqaulTo")
        stack.push("*" + f.asInstanceOf[EqualTo].attribute)
        stack.push("*" + f.asInstanceOf[EqualTo].value.toString())
      }
      
      case GreaterThan(_, _) => { 
        stack.push(":GreaterThan")
        stack.push("*" + f.asInstanceOf[GreaterThan].attribute)
        stack.push("*" + f.asInstanceOf[GreaterThan].value.toString())
      }
      
      case GreaterThanOrEqual(_, _) => {
        stack.push(":GreaterThanOrEqual")
        stack.push("*" + f.asInstanceOf[GreaterThanOrEqual].attribute)
        stack.push("*" + f.asInstanceOf[GreaterThanOrEqual].value.toString())
      }
      case LessThan(_, _) => { 
        stack.push(":LessThan")
        stack.push("*" + f.asInstanceOf[LessThan].attribute)
        stack.push("*" + f.asInstanceOf[LessThan].value.toString())
      }
      case LessThanOrEqual(_, _) => {
        stack.push(":LessThanOrEqual")
        stack.push("*" + f.asInstanceOf[LessThanOrEqual].attribute)
        stack.push("*" + f.asInstanceOf[LessThanOrEqual].value.toString())
      }
      case In(_, _) => {
        /** Transform set of EqualTo **/
        //stack.push("In:")
        var i = 0        
        val col = f.asInstanceOf[In].attribute
        val arrLen = f.asInstanceOf[In].values.length
        for (i <- 0 until arrLen - 1) {
          stack.push(":Or")
        }
        f.asInstanceOf[In].values.foreach{
          x =>
          stack.push(":EqualTo")
          stack.push("*" + col)
          stack.push("*" + x.toString())
        }
      }
      case IsNull(_)=> {
        stack.push(":IsNull")
        stack.push("*" + f.asInstanceOf[IsNull].attribute)
       }
      case IsNotNull(_) => {
        stack.push(":IsNotNull")
        stack.push("*" + f.asInstanceOf[IsNotNull].attribute)
       }
      case StringStartsWith(_, _) => {
        stack.push(":StringStartsWith")
        stack.push("*" + f.asInstanceOf[StringStartsWith].attribute)
        stack.push("*" + f.asInstanceOf[StringStartsWith].value)
      }
      case StringEndsWith(_, _) => {
        stack.push(":StringEndsWith")
        stack.push("*" + f.asInstanceOf[StringEndsWith].attribute)
        stack.push("*" + f.asInstanceOf[StringEndsWith].value)
      }
      case StringContains(_, _) => {
        stack.push(":StringContains")
        stack.push("*" + f.asInstanceOf[StringContains].attribute)
        stack.push("*" + f.asInstanceOf[StringContains].value)
      }
    }
  }
}