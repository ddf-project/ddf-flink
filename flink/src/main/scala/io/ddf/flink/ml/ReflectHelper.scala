package io.ddf.flink.ml

import io.ddf.ml.IModel
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.Predictor

import scala.reflect.api.JavaUniverse

class ReflectHelper(className: String) {
  //, fitOperation: String, predictOperation: String) {

  import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation}

  import scala.reflect.runtime.{universe => ru}
  import ru._

  val fitOpTpe = typeOf[FitOperation[_, _]]
  val predictOpTpe = typeOf[PredictOperation[_, _, _,_]]

  val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
  val mlModuleSymbol = runtimeMirror.staticModule(className)
  val mlModuleClass: ClassSymbol = runtimeMirror.staticClass(className)
  val classMirror = runtimeMirror.reflectClass(mlModuleClass)
  val mlModule = runtimeMirror.reflectModule(mlModuleSymbol).instance

  val singletonMirror = runtimeMirror.reflect(mlModule)

  val ctorSymbol = mlModuleClass.typeSignature.members.filter(m => m.isMethod && m.asMethod.isConstructor && m.asMethod.paramss.isEmpty)

  val objectApply = mlModuleSymbol.typeSignature.member(newTermName("apply")).asMethod
  val modelObject = (singletonMirror.reflectMethod(objectApply))()

  val modelMirror = runtimeMirror.reflect(modelObject)

  val fitMethodSymbol: MethodSymbol = mlModuleClass.typeSignature.member(newTermName("fit")).asMethod
  val fitMethod = modelMirror.reflectMethod(fitMethodSymbol)

  val (fOpSymbol, pOpSymbol) = getMLOps

  val fOp = (getObject(fOpSymbol))

  val pOp = getObject(pOpSymbol)

  def getDataSetType(op: DatasetTypes.EnumVal): Seq[Class[_]] = {
    op match {
      case DatasetTypes.train => getType(fOpSymbol, 1)
      case DatasetTypes.test => getType(pOpSymbol, 1)
      case DatasetTypes.result => getType(pOpSymbol, 2)
    }
  }


  def getObject(symbol: Symbol): Any = {
    if (symbol.isMethod) {
      singletonMirror.reflectMethod(symbol.asMethod).apply()
    } else {
      singletonMirror.reflectField(symbol.asTerm).get
    }
  }

  def getType(msym: Symbol, argPos: Int): Seq[Class[_]] = {

    val (stype, tparams, rtype) = msym.typeSignature match {
      case PolyType(typeParams, resultType) =>
        val rt1 = resultType match {
          case NullaryMethodType(rt) => rt
          case x => x
        }
        ("POLY", Some(typeParams), rt1)
      case NullaryMethodType(resultType) =>
        ("NULLARY", None, resultType)
    }

    val RefinedType(tpe1, _) = rtype
    val ftype = tpe1.tail.head.asInstanceOf[TypeRefApi] //First type is AnyRef, second is the FitOperation
    val retType = ftype.args(argPos) //First type arg is Self and second is Training

    if (retType.typeConstructor.takesTypeArgs) {
      Seq(getClassFromType(retType, tparams)) ++ retType.asInstanceOf[TypeRefApi].args.map(rt => getClassFromType(rt, tparams))
    } else {
      Seq(getClassFromType(retType, tparams))
    }
  }

  def predictOperator: PredictOperation[_, _, _,_] = {
    pOp.asInstanceOf[PredictOperation[ _, _,_,_]]
  }

  private def getClassFromType(tpe: Type, params: Option[List[Symbol]]): Class[_] = {
    val ts: JavaUniverse#Symbol = tpe.typeSymbol
    if (ts.asType.isAbstractType && params.isDefined) {
      val signature: Type = params.get.filter(_ == ts).head.typeSignature
      val bc = signature.baseClasses
      runtimeMirror.runtimeClass(bc.head.asClass)
    } else {
      runtimeMirror.runtimeClass(tpe)
    }
  }

  def train(dataset: DataSet[_], parameterMap: ParameterMap): IModel = {
    fitMethod(dataset, parameterMap, fOp)
    new FlinkModel(modelObject.asInstanceOf[Predictor[_]], this)
  }

  private def getMLOps = {
    case class Operations(fitOp: ru.Symbol, predictOp: ru.Symbol)
    var ops = Operations(null, null)
    val members = mlModuleSymbol.typeSignature.members.filter(m => m.isImplicit && m.isMethod).map(m => m -> m.asInstanceOf[MethodSymbol].returnType)
    members.foreach(println)
    members.foreach {
      case (m, op) if op <:< fitOpTpe =>
        ops = ops.copy(fitOp = m)
      case (m, op) if op <:< predictOpTpe =>
        ops = ops.copy(predictOp = m)
      case _ => //Nothing to do
    }

    (ops.fitOp, ops.predictOp)
  }
}
