package com.zyc.base.util

import org.apache.commons.lang3.StringUtils

object StringDefault {

  /**
    * 判断value 是否为空,如果为空返回默认值空串
    * @param value
    * @param default
    */
  def getDefault(value:String,default:String=""): String ={
    if(StringUtils.isEmpty(value)) default else value
  }

  def isEmpty(value:String): Boolean ={
    StringUtils.isEmpty(value)
  }

}
