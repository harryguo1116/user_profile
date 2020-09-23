package cn.itcast.tags.model

import org.junit.Test

/**
 * @Author Harry
 * @Date 2020-08-31 08:32
 * @Description 编写二分法的代码 并进行测试
 */
object SearchTwoTest {
  /**
   * 依据IP地址Long类型的值到IP地址信息库数组中查找，采用二分查找
   *
   * @param ipLong       IP地址Long类型的值
   * @param ipRulesArray IP 地址信息库数组
   * @return 数组下标索引，如果是-1表示未查到
   */

  def binarySearch(ipLong: Long,
                   ipRulesArray: Array[(Long, Long, String, String)]): Int = {

    //定义起始和结束索引下标
    var startIndex = 0
    var endIndex = ipRulesArray.length - 1
    //循环判断
    while (startIndex <= endIndex) {
      //依据起始索引和结束索引 计算中间索引
      //todo 1、 索引直接相加可能会导致数据溢出
      val middleIndex = startIndex + (endIndex - startIndex) / 2
      //依据中间索引获取数组的值
      val (startIp, endIp, _, _) = ipRulesArray(middleIndex)
      //比较ipLong与获取起始Ip和结束Ip大小
      if (ipLong >= startIp && ipLong <= endIp) {
        return middleIndex
      }
      // 小于起始Ip地址 从左边继续查找
      if (ipLong < startIp) {
        endIndex = middleIndex - 1
      }
      // 大于结束Ip地址 从右边继续查找
      if (ipLong > endIp) {
        startIndex = middleIndex + 1
      }
    }
    //如果未找到 返回-1
    -1
  }
}

