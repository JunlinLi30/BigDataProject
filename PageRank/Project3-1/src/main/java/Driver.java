
public class Driver { // 实现迭代

    public static void main(String[] args) throws Exception {
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        //读进数据是动态的
        String transitionMatrix = args[0];// 命令行路径读进第一个数据--transMatrix
        String prMatrix = args[1];// pagerank, 第一次读进pr1，第二次pr2 --- 动态的 pr0.txt pr1.txt pr2.txt...// pr+i.txt即可
        String unitState = args[2];// subPR 也是动态读进变化的 subPR0，subPR1，... (MR不接受覆盖：output一定要删除，所以无法覆盖前面的subPR，只能建立新的)
        int count = Integer.parseInt(args[3]);// 命令行最后一个argument读进迭代次数30～40左右

        for(int i=0;  i<count;  i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};//第一个MRjob---multiplication unitState + i 其实就是 subPR +i (outputFormat)
            multiplication.main(args1);
            String[] args2 = {unitState + i, prMatrix+(i+1)};
            sum.main(args2);
        }
    }
}
