package cn.itcast;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Scanner;

/**
 * @Author Harry
 * @Date 2020-08-31 14:29
 * @Description
 */
public class BinarySearch {
    //进行二分法查找的前提是数组已经有序！
    public static int rank(int key, int nums[]) {
        //查找范围的上下界
        int low = 0;
        int high = nums.length - 1;
        //未查找到的返回值
        int notFind = -1;
        while (low <= high) {
            //二分中点=数组左边界+(右边界-左边界)/2
            //整数类型默认取下整
            int mid = low + (high - low) / 2;
            //中间值是如果大于key
            if (nums[mid] > key) {
                //证明key在[low,mid-1]这个区间
                //因为num[mid]已经判断过了所以下界要减一
                high = mid - 1;
            } else if (nums[mid] < key) {
                //证明key在[mid+1,high]这个区间
                //同样判断过mid对应的值要从mid+1往后判断
                low = mid + 1;
            } else {
                //查找成功
                return mid;
            }
        }
        //未成功
        return notFind;
    }

    public static void main(String[] args) {
//        System.out.println("请输入数据数量：");
        Scanner scanner = null;
        int amount = 0;
        int nums[] = null ;
        System.out.println("请输入一组数字 输入000表示结束 ");
        while (true) {
            scanner = new Scanner(System.in);
            amount = scanner.nextInt();
            if(000 != amount){
                nums = new int[amount];
            }else break;
        }
        int num;
        int i = 0;
        while (i < amount) {
            nums[i] = scanner.nextInt();
            i++;
        }
        Arrays.sort(nums);
        System.out.println("请输入想要查找的值");
        int key = scanner.nextInt();
        int answer = rank(key, nums);
        if (answer != -1) {
            System.out.println("所查找的数据存在：" + nums[answer]);
        } else {
            System.out.println("您所查找的数据不存在");
        }
//        System.out.println(key);
//        System.out.println(Arrays.toString(nums));
    }
}
