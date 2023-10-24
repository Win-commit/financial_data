## 构思思路
### FileInputFormat
由于Hadoop无法直接对.xlsx文件做处理，所以我们需要对数据做预处理。
有两种方法：

1、自定义InputFormat类与RecordReader类，实现对excel文件的split，输出split文件键值对。

2、将excel文件提前转为.csv文件，这样可以用hadoop里的默认类处理

由于第一种方法需要org.apache.poi库来帮助我们读.xlsx文件，而这个库会有冲突，导致hadoop任务无法正常进行（我也不知道为什么，我没有很好的解决方法）

所以我们采用第二种，这样我们可以直接利用Hadoop内置的KeyValueTextInputFormat来获取split的键值对

### Mapper
mapper的实现很简单，因为我们使用的是KeyValueTextInputFormat，所以传入的是什么键值对直接就输出就ok。

### Reducer
reducer的任务就是去重，对于传入的(key,Iteratable values),我们利用java的Set类实现去重操作，然后对于Set里面的每一个值，我们都将其输出。需要注意的是，Text类应该没有实现Hashcode()与equal()方法，所以Set的元素类型如果是Text的话可能会由于Text对象无法确定是否相等，导致最终的运行结果会有错误。(实践出来的)

## 运行结果
![本地地址](Pictures/fig1.png)
