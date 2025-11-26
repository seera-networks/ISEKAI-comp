// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use crate::{NumOrd, img::Detection};

use super::{jsonize, jsonize_bytes};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionEmscriptenInput {
    pub data: Vec<Vec<u8>>,
    pub args: String,
}
jsonize!(FunctionEmscriptenInput);

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionEmscriptenOutput {
    pub output: Vec<u8>,
}
jsonize!(FunctionEmscriptenOutput);

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionPyodideInput {
    pub data: Vec<String>,
    pub args: String,
}
jsonize!(FunctionPyodideInput);

#[derive(Serialize, Deserialize, Debug)]
pub struct FunctionPyodideOutput {
    pub output: String,
}
jsonize!(FunctionPyodideOutput);

#[derive(Serialize, Deserialize, Debug)]
pub struct GetTicket {
    pub target: String,
    pub column_name: String,
}
jsonize!(GetTicket);

#[derive(Serialize, Deserialize, Debug)]
pub struct SaveResponse {
    pub table_name: String,
}
jsonize_bytes!(SaveResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct MatrixShape {
    pub shape: Vec<usize>,
}
jsonize!(MatrixShape);

#[derive(Serialize, Deserialize, Debug)]
pub struct Histogram {
    pub bins: Vec<f32>,
    pub values: Vec<i32>,
}
jsonize_bytes!(Histogram);

#[derive(Serialize, Deserialize, Debug)]
pub struct HistogramVerifierArg {
    pub min_num_per_bin: i32,
}
jsonize!(HistogramVerifierArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadArg {
    pub target: String,
    pub column_name: String,
}
jsonize!(LoadArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct GetArg {
    pub idx: Vec<usize>,
}
jsonize!(GetArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnArg {
    pub targets: Vec<String>,
}
jsonize!(ColumnArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct IdArg {
    pub rename: Vec<String>,
}
jsonize!(IdArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct StackArg {
    pub axis: usize,
}
jsonize!(StackArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct FullArg {
    pub value: String,
    #[serde(default)]
    pub num: usize,
}
jsonize!(FullArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct ModeArg {
    pub dropna: bool,
}
jsonize!(ModeArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct VarArg {
    pub ddof: usize,
}
jsonize!(VarArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct OperatorArg {
    pub value: f32,
}
jsonize!(OperatorArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct CmpArithArg {
    pub value: f32,
    pub cmp: NumOrd,
}
jsonize!(CmpArithArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct WhereArg {
    pub value: f32,
    pub cmp: NumOrd,
    pub other: f32,
}
jsonize!(WhereArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct CmpStrArg {
    pub value: String,
}
jsonize!(CmpStrArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct TTestArg {
    pub alternative: String,
}
jsonize!(TTestArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct SumArg {
    pub axis: usize,
}
jsonize!(SumArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct RegexArg {
    pub value: String,
}
jsonize!(RegexArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct Boxplot {
    pub min: f32,
    pub lower_quartile: f32,
    pub median: f32,
    pub upper_quartile: f32,
    pub max: f32,

    pub lower_outlier: Vec<f32>,
    pub upper_outlier: Vec<f32>,
}
jsonize_bytes!(Boxplot);

#[derive(Serialize, Deserialize, Debug)]
pub struct TTestResponse {
    pub statistic: f32,
    pub pvalue: f32,
    pub df: f32,
}
jsonize_bytes!(TTestResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct LinearRegressionResponse {
    pub intercept: f32,
    pub params: Vec<f32>,
}
jsonize_bytes!(LinearRegressionResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct TTestLinearRegressionResponse {
    pub ssr: f32,
    pub scale: f32,
    pub bse: HashMap<String, f32>,
    pub tvalues: HashMap<String, f32>,
    pub pvalues: HashMap<String, f32>,
}
jsonize_bytes!(TTestLinearRegressionResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct LogisticRegressionResponse {
    pub intercept: f32,
    pub params: Vec<f32>,
    pub classes: Vec<String>,
}
jsonize_bytes!(LogisticRegressionResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct WaldTestLogisticRegressionResponse {
    pub bse: HashMap<String, f32>,
    pub zvalues: HashMap<String, f32>,
    pub pvalues: HashMap<String, f32>,
}
jsonize_bytes!(WaldTestLogisticRegressionResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct CmpFilterVerifierArg {
    pub minimum_true: i32,
}
jsonize!(CmpFilterVerifierArg);

#[derive(Serialize, Deserialize, Debug)]
pub enum StubOutput {
    Ids(Vec<u64>),
    Record((u64, String, bool)),
    Error(String),
}
jsonize!(StubOutput);

#[derive(Serialize, Deserialize, Debug)]
pub struct StubResponse {
    pub res: HashMap<u64, String>,
}
jsonize!(StubResponse);

#[derive(Serialize, Deserialize, Debug)]
pub struct RunnerResult {
    pub res: Option<StubResponse>,
    pub error: Option<String>,
}
jsonize!(RunnerResult);

#[derive(Serialize, Deserialize, Debug)]
pub struct VerifierKAnonyArg {
    pub k: usize,
}
jsonize!(VerifierKAnonyArg);

#[derive(Serialize, Deserialize, Debug)]
pub struct CountVerifierArgs {
    pub checkpoint: String, // "input" or "output"
    pub minimum_count: usize,
}
jsonize!(CountVerifierArgs);

#[derive(Serialize, Deserialize, Debug)]
pub struct ImgResizeArgs {
    pub width: usize,
    pub height: usize,
}
jsonize!(ImgResizeArgs);

#[derive(Serialize, Deserialize, Debug)]
pub struct Detections {
    pub detections: Vec<Detection>,
}
jsonize!(Detections);

#[derive(Serialize, Deserialize, Debug)]
pub struct ImgBlackoutArgs {
    pub object_labels: Vec<String>,
}
jsonize!(ImgBlackoutArgs);

#[derive(Serialize, Deserialize, Debug)]
pub struct ImgDetectionVerifierRejectArgs {
    pub reject_object_labels: Vec<String>,
}
jsonize!(ImgDetectionVerifierRejectArgs);

#[derive(Serialize, Deserialize, Debug)]
pub struct ImgDetectionVerifierRequireArgs {
    pub black_out_object_labels: Vec<String>,
}
jsonize!(ImgDetectionVerifierRequireArgs);
