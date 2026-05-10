Dag Reference
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
dag.Loc retrieves and returns the data column with the name specified in the second argument from the data source indicated by the data token in the first argument.

dag.Loc is a special node type used to fetch data and cannot be used as a terminal node.
If you want to return fetched data as-is, combine it with dag.Id.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Id(dag.Loc("CqRjmM2HHO", "wage"))) \
    .build()
```

# Id
dag.Id returns the data column of the given node as-is.
You can also provide nodes as an array.

You can rename data columns using the array passed as the second argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Id(dag.Loc("CqRjmM2HHO", "educ"), ["education"])) \
    .add(dag.Id([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")], ["education", "wage"])) \
    .build()
```

# Head
dag.Head returns the specified number of values (second argument) from the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Head(dag.Loc("CqRjmM2HHO", "educ"), 5)) \
    .add(dag.Head([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")], 5)) \
    .build()
```

# Count
dag.Count returns the number of values in the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Count(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Count([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Mean
dag.Mean computes the average value of the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mean(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Mean([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Median
dag.Median computes the median of the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Median(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Median([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Mode
dag.Mode computes the mode of the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mode(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Mode([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Var
dag.Var computes the variance of the data column of the node specified in the first argument.

The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Var(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Var([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Add
dag.Add behaves as follows when only the first argument is provided: it adds the data columns of nodes specified in the array.
When only the first argument is provided, the array must contain exactly two nodes.

If a numeric value is provided as the second argument, that value is added to the data column(s) of the node(s) specified in the first argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Add([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .add(dag.Add(dag.Loc("CqRjmM2HHO", "educ"), 1.0)) \
    .add(dag.Add([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage"), dag.Loc("CqRjmM2HHO", "exper")], 1.0)) \
    .build()
```

# Sub
dag.Sub behaves as follows when only the first argument is provided: it subtracts the data column of the second node from the first node in the array.
When only the first argument is provided, the array must contain exactly two nodes.

If a numeric value is provided as the second argument, that value is subtracted from the data column(s) of the node(s) specified in the first argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Sub([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .add(dag.Sub(dag.Loc("CqRjmM2HHO", "educ"), 1.0)) \
    .add(dag.Sub([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage"), dag.Loc("CqRjmM2HHO", "exper")], 1.0)) \
    .build()
```

# Mul
dag.Mul behaves as follows when only the first argument is provided: it multiplies the data column of the first node by the data column of the second node in the array.
When only the first argument is provided, the array must contain exactly two nodes.

If a numeric value is provided as the second argument, that value is multiplied with the data column(s) of the node(s) specified in the first argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Mul([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .add(dag.Mul(dag.Loc("CqRjmM2HHO", "educ"), 2.0)) \
    .add(dag.Mul([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage"), dag.Loc("CqRjmM2HHO", "exper")], 2.0)) \
    .build()
```

# Div
dag.Div behaves as follows when only the first argument is provided: it divides the data column of the first node by the data column of the second node in the array.
When only the first argument is provided, the array must contain exactly two nodes.

If a numeric value is provided as the second argument, the data column(s) of the node(s) specified in the first argument are divided by that value.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Div([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .add(dag.Div(dag.Loc("CqRjmM2HHO", "educ"), 2.0)) \
    .add(dag.Div([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage"), dag.Loc("CqRjmM2HHO", "exper")], 2.0)) \
    .build()
```

# Log
dag.Log takes the natural logarithm of the data column of the node specified in the first argument.
The first argument can be an array to specify multiple nodes.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Log(dag.Loc("CqRjmM2HHO", "educ"))) \
    .add(dag.Log([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Gt
dag.Gt compares the data column of the node specified in the first argument with the data column of the node specified in the second argument.
It returns true if the first argument is greater, and false otherwise.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Gt(dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage"))) \
    .build()
```

# Full
dag.Full creates a data column filled with the value specified in the first argument.

The length of the data column is the number of rows in the data column of the node specified in the second argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Full("Hello", dag.Loc("CqRjmM2HHO", "educ"))) \
    .build()
```

# Null
dag.Null checks whether the data column of the node specified in the first argument is Null.
It returns true when the value is Null, and false otherwise.
The first argument must be an array.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Null([dag.Loc("CqRjmM2HHO", "educ")])) \
    .add(dag.Null([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Nan
dag.Nan checks whether the data column of the node specified in the first argument is nan.
It returns true when the value is nan, and false otherwise.
The first argument must be an array.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Nan([dag.Loc("CqRjmM2HHO", "educ")])) \
    .add(dag.Nan([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# BoolToStr
dag.BoolToStr creates a string by converting each data value in the node array specified in the first argument:
1 when true, and 0 when false.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.BoolToStr([dag.Gt(dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")), dag.Gt(dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "exper"))])) \
    .build()
```

# Column
dag.Column extracts and returns the column with the name specified in the second argument from the data column of the node in the first argument.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Column(dag.Null([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")]), ["educ"])) \
    .build()
```

# Zip
dag.Zip returns a data column created by combining the data columns of the nodes in the first argument row by row.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Zip([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# Not
dag.Not returns false when the data value of the node specified in the first argument is true, and true when it is false.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Not([dag.Null([dag.Loc("CqRjmM2HHO", "educ")])])) \
    .build()
```

# CmpArith
dag.CmpArith compares the data column of the node in the first argument against the numeric value in the second argument.

The comparison method is specified in the third argument.

If the method is "LT", it returns true when the data value is less than the numeric value, otherwise false.

If the method is "EQ", it returns true when the data value equals the numeric value, otherwise false.

If the method is "GT", it returns true when the data value is greater than the numeric value, otherwise false.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.CmpArith([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "LT")) \
    .add(dag.CmpArith([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "EQ")) \
    .add(dag.CmpArith([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "GT")) \
    .build()
```

# Where
dag.Where compares the data column of the node in the first argument against the numeric value in the second argument,
and replaces values with the fourth argument when the comparison result is false.

The comparison method is specified in the third argument.

If the method is "LT", it returns true when the data value is less than the numeric value, otherwise false.

If the method is "EQ", it returns true when the data value equals the numeric value, otherwise false.

If the method is "GT", it returns true when the data value is greater than the numeric value, otherwise false.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.Where([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "LT", "3.0")) \
    .add(dag.Where([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "EQ", "3.0")) \
    .add(dag.Where([dag.Loc("CqRjmM2HHO", "educ")], 1.0, "GT", "3.0")) \
    .build()
```

# If
dag.If takes an array of three nodes as the first argument.
The first node must be a data column of true/false values.
When true, the value from the second node is selected; when false, the value from the third node is selected.

Example
```
(d, rni) = dag.DAGBuilder() \
    .add(dag.If([dag.CmpArith([dag.Loc("CqRjmM2HHO", "educ")], 15.0, "LT"), dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "wage")])) \
    .build()
```

# LinearRegression
dag.LinearRegression performs linear regression by specifying, in the first argument array, nodes that provide two or more data columns.

The first data column is the dependent variable, and the second and subsequent data columns are explanatory variables.

When executed, it returns JSON containing the intercept and coefficients for explanatory variables.

Example
```
dag_Y = dag.Id(dag.Log(dag.Loc("CqRjmM2HHO", "wage")), ["wage_log"])
dag_X = dag.Id([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "tenure"), dag.Loc("CqRjmM2HHO", "exper")])
(d, rni) = dag.DAGBuilder() \
    .add(dag.LinearRegression([dag_Y, dag_X])) \
    .build()
```

# TTestLinearRegression
dag.TTestLinearRegression executes a t-test for linear regression.
In the first argument array, specify nodes that provide two or more data columns.
In the second argument, specify a node that provides the JSON result of linear regression.

In the first argument array, the first data column is the dependent variable, and the second and subsequent data columns are explanatory variables.
When executed, it returns the t-test result in JSON.

Example
```
dag_Y = dag.Id(dag.Log(dag.Loc("CqRjmM2HHO", "wage")), ["wage_log"])
dag_X = dag.Id([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "tenure"), dag.Loc("CqRjmM2HHO", "exper")])
dag_linear_regression = dag.LinearRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.TTestLinearRegression([dag_Y, dag_X], dag_linear_regression)) \
    .build()
```

# LogisticRegression
dag.LogisticRegression performs logistic regression by specifying, in the first argument array, nodes that provide two or more data columns.

The first data column is the dependent variable, and the second and subsequent data columns are explanatory variables.

When executed, it returns JSON containing the intercept and coefficients for explanatory variables.

Example
```
dag_Y = dag.Id(dag.CmpArith(dag.Loc("CqRjmM2HHO", "wage"), mean_wage, "GE"), ["wage_gt_mean"])
dag_X = dag.Id([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "tenure"), dag.Loc("CqRjmM2HHO", "exper")])
dag_logistic_regression = dag.LogisticRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.LogisticRegression([dag_Y, dag_X])) \
    .build()
```

# WaldTestLogisticRegression
dag.WaldTestLogisticRegression executes the Wald test for logistic regression.
In the first argument array, specify nodes that provide one or more data columns.
In the second argument, specify a node that provides the JSON result of logistic regression.

In the first argument array, the first and subsequent data columns are explanatory variables.

When executed, it returns the Wald test result in JSON.

Example
```
dag_Y = dag.Id(dag.CmpArith(dag.Loc("CqRjmM2HHO", "wage"), mean_wage, "GE"), ["wage_gt_mean"])
dag_X = dag.Id([dag.Loc("CqRjmM2HHO", "educ"), dag.Loc("CqRjmM2HHO", "tenure"), dag.Loc("CqRjmM2HHO", "exper")])
dag_logistic_regression = dag.LogisticRegression([dag_Y, dag_X])
(d, rni) = dag.DAGBuilder() \
    .add(dag.WaldTestLogisticRegression(dag_X, dag_logistic_regression)) \
    .build()
```
