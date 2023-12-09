package com.sparkdataframes.Course_sparkbyexamples.L16_Joins


/**
 *
 */

case class VariableWrapper(explicitArgs: ExplicitArgs) {

	object Vars {

		import explicitArgs._

		implicit val implicitArgs: ImplicitArgs = new ImplicitArgs(leftDF, rightDF, leftColname, givenLeftDataType, rightColname, givenRightDataType)

		val dcommon = CommonDecoderCheckSpecs()
		val d1 = DecoderCheck1_Canonical_AvroStringToJsonString()
		val d2 = DecoderCheck2_JsonSkeuoToAvroString()
		val d3a = DecoderCheck3a_JsonStringBegin_JsonDecoderVsJsonTrans()
		val d3b = DecoderCheck3b_JsonStringBegin_AvroDecoderVsAvroTrans()
		val d3c = DecoderCheck3c_AvroStringBegin_AvroDecoderVsAvroTrans()
		val d3d = DecoderCheck3d_AvroStringBegin_JsonDecoderVsJsonTrans()
	}
}