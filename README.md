# MQTT工具类

## 描述

* MQTT Client 通用工具类

## 环境

* jdk8 x64 及以上版本

## 依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-excel</artifactId>
    <!-- {paho.version}_{util.version}_{jdk.version} -->
    <version>1.2.5_1.0_jdk8</version>
    <classifier>shaded</classifier>
</dependency>
```

## 例子

```java
ExcelUtil excelUtil = ExcelUtil.builder().build();

@Test
void 读取一个exxcel返回ListMap() {
    List<Map<String, Object>> list = excelUtil.read(Paths.get("d:/tmp/发货明细/20260227/20260226发货明细.xlsx"), 0, 1, 1);
    for (Map<String, Object> m : list) {
        log.info("{}", m);
        log.info("{}", m.get("工况号"));
    }
}

@Test
void 读取一个excel自己处理细节() {
    excelUtil.read(Paths.get("d:/tmp/excel/2026016发货明细.xlsx"), excelReader -> excelReader
            .sheet(0)
            .asFullSheet()
            .copyOnMerged() // <- 转为FullSheet并复制合并单元格
            .header(1, 1)
            .rows()
            .map(row -> row.too(FaHuo.class))
            .forEach(new Consumer<FaHuo>() {
                @Override
                public void accept(FaHuo faHuo) {
                    log.info("{} {} {}", faHuo.getIccid(), faHuo.getSimMsisdn(), faHuo.getExprTime());
                }
            }));
}

@Test
void 读取一个excel自己处理细节2() {
    excelUtil.read(Paths.get("d:/tmp/excel/2026016发货明细.xlsx"), excelReader -> excelReader
            .sheet(0)
            .asFullSheet()
            .copyOnMerged() // <- 转为FullSheet并复制合并单元格
            .header(1, 1)
            .rows()
            .map(Row::toMap)
            .forEach(row -> {
                log.debug("{}",row);
            }));
}

@Test
void 写出一个Sheet使用对象数组() {
    // 准备导出数据
    List<Object> rows = new ArrayList<>();
    rows.add(new String[] { "列1", "列2", "列3" });
    rows.add(new int[] { 1, 2, 3, 4 });
    rows.add(new Object[] { 5, new Date(), 7, null, "字母", 9, 10.1243 });
    excelUtil.write(Paths.get("d:/tmp"), "test", new SimpleSheet<Object>(rows));
}

@Test
void 写出一个动态列() {
    excelUtil.write(Paths.get("d:/tmp"), "test", new DynamicColumnListMapSheet<Object>() {
        int page = 1;

        @Override
        protected List<Map<String, Object>> more() {
            return getRows(page++);
        }
    }.setName("大量数据"));
}

@Test
void 写出固定列() {
    excelUtil.write(Paths.get("d:/tmp"), "test", new AutoNumberedListMapSheet<Object>() {
        int page = 1;

        @Override
        protected List<Map<String, Object>> more() {
            return getRows(page++);
        }
    }.setName("固定列数据"));
}
```

