Dagリファレンス
---
<!-- TOC -->
- [Loc](#loc)
- [Id](#id)
- [Head](#head)
- [Count](#count)
- [Mean](#mean)
- [Median](#median)
- [Mode](#mode)
- [Var](#var)
- [Add](#add)
- [Sub](#sub)
- [Mul](#mul)
- [Div](#div)
- [Log](#log)
- [Gt](#gt)
- [Full](#full)
- [Null](#null)
- [Nan](#nan)
- [BoolToStr](#booltostr)
- [Column](#column)
- [Zip](#zip)
- [Not](#not)
- [CmpArith](#cmparith)
- [Where](#where)
- [If](#if)
- [LinearRegression](#linearregression)
- [TTestLinearRegression](#ttestlinearregression)
- [LogisticRegression](#logisticregression)
- [WaldTestLogisticRegression](#waldtestlogisticregression)
<!-- /TOC -->
# Loc
dag.Locは与えられた引数の名前のデータ列を返します。

dag.Locはデータを取得する特別な種類のノードで、末端ノードに指定できません。
取得したデータをそのまま返したい場合はdag.Idと組み合わせます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Id(dag.Loc("wage"))) \
    .build()
```

# Id
dag.Idは与えられたノードのデータ列をそのまま返します。
ノードは配列で与えることも可能です。

2番目の引数の配列でデータ列の名前を変更することができます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Id(dag.Loc("educ"), ["education"])) \
    .add(dag.Id([dag.Loc("educ"), dag.Loc("wage")], ["education", "wage"])) \
    .build()
```

# Head
dag.Headは第一引数で指定したノードのデータ列を第2引数で指定した数字の数だけ返します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Head(dag.Loc("educ"), 5)) \
    .add(dag.Head([dag.Loc("educ"), dag.Loc("wage")], 5)) \
    .build()
```

# Count
dag.Countは第一引数で指定したノードのデータ列の個数を返します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Count(dag.Loc("educ"))) \
    .add(dag.Count([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Mean
dag.Meanは第一引数で指定したノードのデータ列の平均値を計算します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mean(dag.Loc("educ"))) \
    .add(dag.Mean([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Median
dag.Medianは第一引数で指定したノードのデータ列の中央値を計算します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Median(dag.Loc("educ"))) \
    .add(dag.Median([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Mode
dag.Modeは第一引数で指定したノードのデータ列の最頻値を計算します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mode(dag.Loc("educ"))) \
    .add(dag.Mode([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Var
dag.Varは第一引数で指定したノードのデータ列の分散を計算します。

第一引数は配列とし、複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Var(dag.Loc("educ"))) \
    .add(dag.Var([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Add
dag.Addは第一引数のみの場合、配列で指定したノードのデータ列を加算します。
第一引数のみの場合、配列の個数は2個でなければなりません。

第二引数で数値を与えると、その数値を第一引数で指定したノードのデータ列に加算します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Add([dag.Loc("educ"), dag.Loc("wage")])) \
    .add(dag.Add(dag.Loc("educ"), 1.0)) \
    .add(dag.Add([dag.Loc("educ"), dag.Loc("wage"), dag.Loc("exper")], 1.0)) \
    .build()
```

# Sub
dag.Subは第一引数のみの場合、配列で指定した最初のノードのデータ列から2番目のノードのデータ列を減算します。
第一引数のみの場合、配列の個数は2個でなければなりません。

第二引数で数値を与えると、その数値を第一引数で指定したノードのデータ列から減算します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Sub([dag.Loc("educ"), dag.Loc("wage")])) \
    .add(dag.Sub(dag.Loc("educ"), 1.0)) \
    .add(dag.Sub([dag.Loc("educ"), dag.Loc("wage"), dag.Loc("exper")], 1.0)) \
    .build()
```

# Mul
dag.Mulは第一引数のみの場合、配列で指定した最初のノードのデータ列と2番目のノードのデータ列を乗算します。
第一引数のみの場合、配列の個数は2個でなければなりません。

第二引数で数値を与えると、その数値を第一引数で指定したノードのデータ列に乗算します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mul([dag.Loc("educ"), dag.Loc("wage")])) \
    .add(dag.Mul(dag.Loc("educ"), 2.0)) \
    .add(dag.Mul([dag.Loc("educ"), dag.Loc("wage"), dag.Loc("exper")], 2.0)) \
    .build()
```

# Div
dag.Divは第一引数のみの場合、配列で指定した最初のノードのデータ列を2番目のノードのデータ列で割ります
第一引数のみの場合、配列の個数は2個でなければなりません。

第二引数で数値を与えると、その数値で第一引数で指定したノードのデータ列を割ります。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Div([dag.Loc("educ"), dag.Loc("wage")])) \
    .add(dag.Div(dag.Loc("educ"), 2.0)) \
    .add(dag.Div([dag.Loc("educ"), dag.Loc("wage"), dag.Loc("exper")], 2.0)) \
    .build()
```

# Log
dag.Logは第一引数で指定したノードのデータ列の自然対数を取ります。
第一引数は配列にして複数のノードを指定することもできます。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Log(dag.Loc("educ"))) \
    .add(dag.Log([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Gt
dag.Gtは第一引数で指定したノードのデータ列と第二引数で指定したノードのデータ列を比較し、
第一引数が大きければtrueを返し、小さければfalseを返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Gt(dag.Loc("educ"), dag.Loc("wage"))) \
    .build()
```

# Full
dag.Fullは第一引数で指定した値でデータ列を作ります。

データ列の長さは第二引数で指定したノードのデータ列の個数になります。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Full("Hello", dag.Loc("educ"))) \
    .build()
```

# Null
dag.Nullは第一引数で指定したノードのデータ列がNullかどうかをチェックし、
Nullの場合はtrueを返し、Nullでない場合はfalseを返します。
第一引数は配列でなければなりません。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Null([dag.Loc("educ")])) \
    .add(dag.Null([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Nan
dag.Nanは第一引数で指定したノードのデータ列がnanかどうかをチェックし、
nanの場合はtrueを返し、nanでない場合はfalseを返します。
第一引数は配列でなければなりません。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Nan([dag.Loc("educ")])) \
    .add(dag.Nan([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# BoolToStr
dag.BoolToStrは、第一引数で指定されたノードの配列のデータ列の値がtrueの場合1、
falseの場合0として文字列を作成します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.BoolToStr([dag.Gt(dag.Loc("educ"), dag.Loc("wage")), dag.Gt(dag.Loc("educ"), dag.Loc("exper"))])) \
    .build()
```

# Column
dag.Columnは第一引数のノードのデータ列から、第2引数で指定された名前の列を取り出して返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Column(dag.Null([dag.Loc("educ"), dag.Loc("wage")]), ["educ"])) \
    .build()
```

# Zip
dag.Zipは第一引数のノードのデータ列を一行ごとに結合したデータ列を返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Zip([dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# Not
dag.Notは第一引数のノードのデータ列がtrueであればfalse、falseであればtrueを返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Not([dag.Null([dag.Loc("educ")])])) \
    .build()
```

# CmpArith
dag.CmpArithは第一引数のノードのデータ列を第二引数の数値を使って比較します。

比較の手段は第三引数で指定します。

手段が"LT"だと、データ列が数値よりも小さい場合にtrue、そうでない場合にfalseを返します。

手段が"EQ"だと、データ列が数値と等しい場合にtrue、そうでない場合にfalseを返します。

手段が"GT"だと、データ列が数値よりも大きい場合にtrue、そうでない場合にfalseを返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.CmpArith([dag.Loc("educ")], 1.0, "LT")) \
    .add(dag.CmpArith([dag.Loc("educ")], 1.0, "EQ")) \
    .add(dag.CmpArith([dag.Loc("educ")], 1.0, "GT")) \
    .build()
```

# Where
dag.Whereは第一引数のノードのデータ列を第二引数の数値を使って比較し、
その結果がfalseの場合、第4引数で置き換えます。

比較の手段は第三引数で指定します。

手段が"LT"だと、データ列が数値よりも小さい場合にtrue、そうでない場合にfalseを返します。

手段が"EQ"だと、データ列が数値と等しい場合にtrue、そうでない場合にfalseを返します。

手段が"GT"だと、データ列が数値よりも大きい場合にtrue、そうでない場合にfalseを返します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Where([dag.Loc("educ")], 1.0, "LT", "3.0")) \
    .add(dag.Where([dag.Loc("educ")], 1.0, "EQ", "3.0")) \
    .add(dag.Where([dag.Loc("educ")], 1.0, "GT", "3.0")) \
    .build()
```

# If
dag.Ifは第一引数の配列で3つのノードを指定します。
第一のノードはtrue/falseで構成されるデータ列です。
trueの場合、第二のノードのデータ列の値を選択し、falseの場合、第三のノードのデータ列の値を選択します。

例
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.If([dag.CmpArith([dag.Loc("educ")], 15.0, "LT"), dag.Loc("educ"), dag.Loc("wage")])) \
    .build()
```

# LinearRegression
dag.LinearRegressionは第一引数の配列で2つ以上のデータ列を提供するノードを指定して線形回帰を実行します。

第一のデータ列は目的変数、第二以降のデータ列は説明変数です。

実行すると、切片と説明変数の係数を記録したJSONを返します。

例
```
dag_Y = dag.Id(dag.Log(dag.Loc("wage")), ["wage_log"])
dag_X = dag.Id([dag.Loc("educ"), dag.Loc("tenure"), dag.Loc("exper")])
(d, rni) = dag.DAGBuilder() \
    .add(dag.LinearRegression([dag_Y, dag_X])) \
    .build()
```

# TTestLinearRegression
dag.TTestLinearRegressionは第一引数の配列で2つ以上のデータ列を提供するノードを指定、
第二引数で線形回帰の結果のJSONを提供するノードを指定し、線形回帰のT検定を実行します。

第一引数の配列の第一のデータ列は目的変数、第一引数の配列の第二以降のデータ列は説明変数です。
実行すると、T検定の結果をJSONで返します。

例
```
dag_Y = dag.Id(dag.Log(dag.Loc("wage")), ["wage_log"])
dag_X = dag.Id([dag.Loc("educ"), dag.Loc("tenure"), dag.Loc("exper")])
dag_linear_regression = dag.LinearRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.TTestLinearRegression([dag_Y, dag_X], dag_linear_regression)) \
    .build()
```

# LogisticRegression
dag.LogisticRegressionは第一引数の配列で2つ以上のデータ列を提供するノードを指定して、
ロジスティクス回帰を実行します。

第一のデータ列は目的変数、第二以降のデータ列は説明変数です。

実行すると、切片と説明変数の係数を記録したJSONを返します。

例
```
dag_Y = dag.Id(dag.CmpArith(dag.Loc("wage"), mean_wage, "GE"), ["wage_gt_mean"])
dag_X = dag.Id([dag.Loc("educ"), dag.Loc("tenure"), dag.Loc("exper")])
dag_logistic_regression = dag.LogisticRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.LogisticRegression([dag_Y, dag_X])) \
    .build()
```

# WaldTestLogisticRegression
dag.WaldTestLogisticRegressionは第一引数の配列で一つ以上のデータ列を提供するノードを指定、
第二引数で線形回帰の結果のJSONを提供するノードを指定し、ロジスティック回帰のWald検定を実行します。

第一引数の配列の第一以降のデータ列は説明変数です。

実行すると、Wald検定の結果をJSONで返します。

例
```
dag_Y = dag.Id(dag.CmpArith(dag.Loc("wage"), mean_wage, "GE"), ["wage_gt_mean"])
dag_X = dag.Id([dag.Loc("educ"), dag.Loc("tenure"), dag.Loc("exper")])
dag_logistic_regression = dag.LogisticRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.WaldTestLogisticRegression(dag_X, dag_logistic_regression)) \
    .build()
```