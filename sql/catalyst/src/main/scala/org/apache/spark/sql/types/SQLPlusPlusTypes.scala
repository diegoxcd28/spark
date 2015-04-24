package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion

import scala.math.BigDecimal


/**
 * Created by diegoxcd on 4/1/15.
 */
object SQLPlusPlusTypes {

  // The conversion for integral and floating point types have a linear widening hierarchy:
  private val typePrecedence =
    Seq(NullType,BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited, StringType, ArrayType, StructType)

  def typeOrder(t1: DataType): Int = typePrecedence.indexOf(t1)

  def coerceAnyNumeric(evalE:Any, t1:DataType): (Any,DataType) ={
    t1 match {
      case t: AnyType => evalE match {
        case exp: String =>
          try {
            (exp.toDouble, DoubleType)
          }catch {
            case e: NumberFormatException => (exp,StringType)
          }
        case e => (e,ScalaReflection.typeOfObject(e))
      }
      case t: NumericType => (evalE,t)
      case other => sys.error(s"Type $other does not support numeric operations")
    }
  }

  def coerceAny(evalE:Any, t1:DataType): (Any,DataType) = {
    t1 match {
      case t: AnyType => (evalE,ScalaReflection.typeOfObject(evalE))
      case t => (evalE, t)
    }
  }

  def convertToType(value: Any, from: NumericType, to: NumericType) : Any = {
    if (from == to) {
      value
    } else
      to match {
        case ByteType    => from.numeric.asInstanceOf[Numeric[Any]].toInt(value).toByte
        case ShortType   => from.numeric.asInstanceOf[Numeric[Any]].toInt(value).toShort
        case IntegerType => from.numeric.asInstanceOf[Numeric[Any]].toInt(value)
        case LongType    => from.numeric.asInstanceOf[Numeric[Any]].toLong(value)
        case FloatType   => from.numeric.asInstanceOf[Numeric[Any]].toFloat(value)
        case DoubleType  => from.numeric.asInstanceOf[Numeric[Any]].toDouble(value)
        case t:DecimalType => try {
          changePrecision(Decimal(from.numeric.asInstanceOf[Numeric[Any]].toDouble(value)), t)
        } catch {
          case _: NumberFormatException => null
        }
      }

  }
  private[this] def changePrecision(value: Decimal, decimalType: DecimalType): Decimal = {
    decimalType match {
      case DecimalType.Unlimited =>
        value
      case DecimalType.Fixed(precision, scale) =>
        if (value.changePrecision(precision, scale)) value else null
    }
  }
  def findTightestCommonType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonType(t1,t2).getOrElse(AnyType)
  }

  object AnyComparator extends Ordering[Any] {
    override def compare(x: Any, y: Any): Int = {
      val (nx, nt1) = coerceAny(x, AnyType)
      val (ny, nt2) = coerceAny(y, AnyType)
      val t = findTightestCommonType(nt1, nt2)
      (t, nt1, nt2) match {
        case (n: NumericType, nt1: NumericType, nt2: NumericType) =>
          n.numeric.compare(SQLPlusPlusTypes.convertToType(nx, nt1, n).asInstanceOf[n.JvmType],
            convertToType(ny, nt2, n).asInstanceOf[n.JvmType])
        case (n: NativeType, nt1: NativeType, nt2: NativeType) if nt1 == nt2 =>
          n.ordering.compare(nx.asInstanceOf[n.JvmType], ny.asInstanceOf[n.JvmType])
        case (_, _, _) =>
          IntegerType.ordering.compare(typeOrder(nt1),
            typeOrder(nt2))
      }
    }
  }


  trait AnyNumeric extends Numeric[Any] {
    override def plus(x: Any, y: Any): Any =
      transform[NumericType](x,y) match {
        case None  => null
        case Some((n,a,b)) => n.numeric.plus(a.asInstanceOf[n.JvmType],b.asInstanceOf[n.JvmType])
      }

    override def times(x: Any, y: Any): Any =
      transform[NumericType](x,y) match {
        case None => null
        case Some((n,a,b)) => n.numeric.times(a.asInstanceOf[n.JvmType],b.asInstanceOf[n.JvmType])
      }

    override def minus(x: Any, y: Any): Any =
      transform[NumericType](x,y) match {
        case None => null
        case Some((n,a,b)) => n.numeric.minus(a.asInstanceOf[n.JvmType],b.asInstanceOf[n.JvmType])
      }

    override def negate(x: Any): Any =
      transform(x) match {
        case None => null
        case Some((n,a)) => n.numeric.negate(a.asInstanceOf[n.JvmType])
      }


    override def toDouble(x: Any): Double = transform(x) match {
      case None => 0.0
      case Some((n,a)) => n.numeric.toDouble(a.asInstanceOf[n.JvmType])
    }
    override def toFloat(x: Any): Float = transform(x) match {
      case None => 0.0.toFloat
      case Some((n,a)) => n.numeric.toFloat(a.asInstanceOf[n.JvmType])
    }
    override def toInt(x: Any): Int = transform(x) match {
      case None => 0
      case Some((n,a)) => n.numeric.toInt(a.asInstanceOf[n.JvmType])
    }
    override def toLong(x: Any): Long = transform(x) match {
      case None => 0
      case Some((n,a)) => n.numeric.toLong(a.asInstanceOf[n.JvmType])
    }
    override def fromInt(x: Int): Any = x

    override def compare(x: Any, y: Any): Int = AnyComparator.compare(x,y)

    def transform[T <: NumericType](x: Any, y: Any): Option[(T,Any,Any)] ={
      val (nEvalE1, nt1) = coerceAnyNumeric(x,AnyType)
      val (nEvalE2, nt2) = coerceAnyNumeric(y,AnyType)
      val t = findTightestCommonType(nt1,nt2)
      (t,nt1,nt2) match {
        case (n:NumericType,nt1:NumericType, nt2: NumericType)  =>
          Some(n.asInstanceOf[T],SQLPlusPlusTypes.convertToType(nEvalE1,nt1,n),
            SQLPlusPlusTypes.convertToType(nEvalE2,nt2,n))
        case _ => None
      }
    }
    def transform(x: Any): Option[(NumericType,Any)] ={
      val (nEvalE1, t) = coerceAnyNumeric(x,AnyType)
      t match {
        case n:NumericType  =>
          Some(n,nEvalE1)
        case _ => None
      }
    }
  }

  object AnyFractional extends AnyNumeric with Fractional[Any]  {
    override def div(x: Any, y: Any): Any =
      transform[NumericType](x,y) match {
        case None => null
        case Some((n,a,b)) =>
          if (b != 0 ) {
            n match {
              case n: FractionalType =>
                n.fractional.div(a.asInstanceOf[n.JvmType], b.asInstanceOf[n.JvmType])
              case n: IntegralType =>
                n.integral.quot(a.asInstanceOf[n.JvmType], b.asInstanceOf[n.JvmType])
            }
          }
          else null
      }
  }
  object AnyIntegral extends AnyNumeric with Integral[Any]  {

    override def quot(x: Any, y: Any): Any = transform[IntegralType](x,y) match {
      case None => null
      case Some((n,a,b)) =>
        if (b != 0 ) n.integral.quot(a.asInstanceOf[n.JvmType],b.asInstanceOf[n.JvmType])
        else null
    }



    override def rem(x: Any, y: Any): Any = transform[NumericType](x,y) match {
      case None => null
      case Some((n,a,b)) =>
        if (b != 0 ) {
          n match {
            case n: FractionalType =>
              n.asIntegral.rem(a.asInstanceOf[n.JvmType], b.asInstanceOf[n.JvmType])
            case n: IntegralType =>
              n.integral.rem(a.asInstanceOf[n.JvmType], b.asInstanceOf[n.JvmType])
          }
        }
        else null
    }
  }
}