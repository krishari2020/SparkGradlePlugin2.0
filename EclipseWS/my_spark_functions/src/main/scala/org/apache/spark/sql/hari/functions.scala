package org.apache.spark.sql.hari

import org.apache.spark.sql.catalyst.expressions.{ Hour, UnaryExpression, ImplicitCastInputTypes, TimeZoneAwareExpression, Expression, NullIntolerant }
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode, CodegenFallback }
import org.apache.spark.sql.types._
import java.lang.Long
import java.sql.Timestamp
import java.util.{ Calendar, Date, TimeZone }
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ ExpressionDescription, NullIntolerant }

object functions {

  /**
   * Given a timestamp value and a timezone returns true if the 'hour of the day' value falls between 0 (inclusive) and 12
   *
   * @param inpCol - input column of type Timestamp.
   * @param timeZoneId - Id of the TimeZone , default value is UTC
   */

  def isMorning(inpCol: Column, timeZoneId: String = "UTC"): Column = Column(Morning(inpCol.expr, Some(timeZoneId)))

  /**
   * Given a timestamp value and a timezone returns true if the 'hour of the day' value falls between 12 (inclusive) and 16
   *
   * @param inpCol - input column of type Timestamp.
   * @param timeZoneId - Id of the TimeZone , default value is UTC
   */

  def isAfternoon(inpCol: Column, timeZoneId: String = "UTC"): Column = Column(Afternoon(inpCol.expr, Some(timeZoneId)))

  /**
   * Given a timestamp value and a timezone returns true if the 'hour of the day' value falls between 16 (inclusive) and 20
   *
   * @param inpCol - input column of type Timestamp.
   * @param timeZoneId - Id of the TimeZone , default value is UTC
   */

  def isEvening(inpCol: Column, timeZoneId: String = "UTC"): Column = Column(Evening(inpCol.expr, Some(timeZoneId)))

  /**
   * Given a timestamp value and a timezone returns true if the 'hour of the day' value falls between 20 (inclusive) and 24
   *
   * @param inpCol - input column of type Timestamp.
   * @param timeZoneId - Id of the TimeZone , default value is UTC
   */

  def isNight(inpCol: Column, timeZoneId: String = "UTC"): Column = Column(Night(inpCol.expr, Some(timeZoneId)))

}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns true if the 'hour of the day' value falls between 0(inclusive) and 12.",
  examples = """
    Examples:
      > SELECT _FUNC_('2020-06-30 10:00:00');
       true
  """)
case class Morning(child: Expression, timeZoneId: Option[String] = None) extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression with NullIntolerant {

  lazy val hours: Hour = Hour(child, timeZoneId)

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = hours.inputTypes

  override def nullSafeEval(timestamp: Any): Any = {
    val hourOfDay = DateTimeUtils.getHours(timestamp.asInstanceOf[Long], timeZone)
    hourOfDay match {
      case x if x >= 0 && x < 12 => true
      case default               => false
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Option(timeZoneId))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => "(" + ctx.genEqual(IntegerType, s"$dtu.getHours($c,$tz)", "0") + " || " + ctx.genGreater(IntegerType, s"$dtu.getHours($c, $tz)", "0") + s") && $dtu.getHours($c, $tz) < 12L")
  }

}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns true if the 'hour of the day' value falls between 12(inclusive) and 16.",
  examples = """
    Examples:
      > SELECT _FUNC_('2020-06-30 14:00:00');
       true
  """)
case class Afternoon(child: Expression, timeZoneId: Option[String] = None) extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression with NullIntolerant {

  lazy val hours: Hour = Hour(child, timeZoneId)

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = hours.inputTypes

  override def nullSafeEval(timestamp: Any): Any = {
    val hourOfDay = DateTimeUtils.getHours(timestamp.asInstanceOf[Long], timeZone)
    hourOfDay match {
      case x if x >= 12 && x < 16 => true
      case default                => false
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Option(timeZoneId))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => "(" + ctx.genEqual(IntegerType, s"$dtu.getHours($c, $tz)", "12") + " || " + ctx.genGreater(IntegerType, s"$dtu.getHours($c, $tz)", "12") + s") && $dtu.getHours($c, $tz) < 16")
  }

}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns true if the 'hour of the day' value falls between 16(inclusive) and 20.",
  examples = """
    Examples:
      > SELECT _FUNC_('2020-06-30 18:00:00');
       true
  """)
case class Evening(child: Expression, timeZoneId: Option[String] = None) extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression with NullIntolerant {

  override def dataType: DataType = BooleanType

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long], timeZone) match {
      case x if x >= 16 && x < 20 => true
      case default                => false
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => "(" + ctx.genEqual(IntegerType, s"$dtu.getHours($c,$tz)", "16") + " || " + ctx.genGreater(LongType, s"$dtu.getHours($c, $tz)", "16") + s") && $dtu.getHours($c, $tz) < 20")
  }

}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns true if the 'hour of the day' value falls between 20(inclusive) and 24.",
  examples = """
    Examples:
      > SELECT _FUNC_('2020-06-30 22:00:00');
       true
  """)
case class Night(child: Expression, timeZoneId: Option[String] = None) extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression with NullIntolerant {

  override def dataType: DataType = BooleanType

  def this(child: Expression) = this(child, None)

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long], timeZone) match {
      case x if x >= 20 && x < 24 => true
      case default                => false
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", timeZone)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => "(" + ctx.genEqual(IntegerType, s"$dtu.getHours($c,$tz)", "20") + " || " + ctx.genGreater(LongType, s"$dtu.getHours($c, $tz)", "20") + s") && $dtu.getHours($c, $tz) < 24")
  }
}

