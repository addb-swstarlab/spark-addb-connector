package kr.ac.yonsei.delab.addb_srconnector.util

import scala.collection.mutable.Stack
import org.apache.spark.sql.sources._
import kr.ac.yonsei.delab.addb_srconnector.{RedisTableList, RedisTable}

object Filters { 
    def makeFilterString(f: Filter, stack: Stack[String], tableID: Int, table:RedisTable) : Unit = {
      
      // Since "column name" should be converted into "column index"
      // get Table's Column name with index from RedisTableList object
      var columnNameWithIndex = RedisTableList.getTableColumnWithIndex(tableID, table)
      
    f match {
      case Or(_,_) =>  { 
        stack.push("Or:")
        makeFilterString(f.asInstanceOf[Or].left, stack, tableID, table)
        makeFilterString(f.asInstanceOf[Or].right, stack, tableID, table)
      }
      case And(_,_) =>  {
        stack.push("And:")
        makeFilterString(f.asInstanceOf[And].left, stack, tableID, table)
        makeFilterString(f.asInstanceOf[And].right, stack, tableID, table)
      }
      
      case EqualTo(_,_) => {
        stack.push("EqualTo:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[EqualTo].attribute).get + "*")
        stack.push(f.asInstanceOf[EqualTo].value.toString() + "*")
      }
      
      case GreaterThan(_, _) => { 
        stack.push("GreaterThan:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[GreaterThan].attribute).get + "*")
        stack.push(f.asInstanceOf[GreaterThan].value.toString() + "*")
      }
      
      case GreaterThanOrEqual(_, _) => {
        stack.push("GreaterThanOrEqual:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[GreaterThanOrEqual].attribute).get + "*")
        stack.push(f.asInstanceOf[GreaterThanOrEqual].value.toString() + "*")
      }
      case LessThan(_, _) => { 
        stack.push("LessThan:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[LessThan].attribute).get + "*")
        stack.push(f.asInstanceOf[LessThan].value.toString() + "*")
      }
      case LessThanOrEqual(_, _) => {
        stack.push("LessThanOrEqual:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[LessThanOrEqual].attribute).get + "*")
        stack.push(f.asInstanceOf[LessThanOrEqual].value.toString() + "*")
      }
      case In(_, _) => {
        /** Transform set of EqualTo **/
        //stack.push("In:")
        var i = 0        
        val col = columnNameWithIndex.get(f.asInstanceOf[In].attribute).get
        val arrLen = f.asInstanceOf[In].values.length
        for (i <- 0 until arrLen - 1) {
          stack.push("Or:")
        }
        f.asInstanceOf[In].values.foreach{
          x =>
          stack.push("EqualTo:")
          stack.push(col + "*")
          stack.push(x.toString() + "*")
        }
      }
      case IsNull(_)=> {
        stack.push("IsNull:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[IsNull].attribute).get + "*")
       }
      case IsNotNull(_) => {
        stack.push("IsNotNull:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[IsNotNull].attribute).get + "*")
       }
      case StringStartsWith(_, _) => {
        stack.push("StringStartsWith:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[StringStartsWith].attribute).get + "*")
        stack.push(f.asInstanceOf[StringStartsWith].value + "*")
      }
      case StringEndsWith(_, _) => {
        stack.push("StringEndsWith:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[StringEndsWith].attribute).get + "*")
        stack.push(f.asInstanceOf[StringEndsWith].value + "*")
      }
      case StringContains(_, _) => {
        stack.push("StringContains:")
        stack.push(columnNameWithIndex.get(f.asInstanceOf[StringContains].attribute).get + "*")
        stack.push(f.asInstanceOf[StringContains].value + "*")
      }
    }
  }
}