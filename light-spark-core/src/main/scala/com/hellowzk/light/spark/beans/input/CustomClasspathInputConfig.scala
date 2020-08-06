package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.CustomClasspathInputWorker

class CustomClasspathInputConfig extends CustomInputConfig {
  setWorkerClass(classOf[CustomClasspathInputWorker].getName)
}
