package myutils

import com.ggstar.util.ip.IpHelper

/*
* https://github.com/wzhe06/ipdatabase
* */

object IpUtils {
  def getCity(ip: String) = {
      IpHelper.findRegionByIp(ip)
  }

 /* def main(args: Array[String]): Unit = {
    println(getCity("218.22.9.56"))
  }*/
}
