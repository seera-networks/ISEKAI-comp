// SPDX-FileCopyrightText: 2025 SEERA Networks Corporation <info@seera-networks.com>
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

#[macro_export]
macro_rules! jsonize {
    ($structname: ident) => {
        impl $structname {
            #[allow(dead_code)]
            pub fn to_json(self: &Self) -> String {
                serde_json::to_string(self).unwrap()
            }

            #[allow(dead_code)]
            pub fn from_json(json: &str) -> Self {
                serde_json::from_str(json).unwrap()
            }
        }
    };
}

#[macro_export]
macro_rules! jsonize_bytes {
    ($structname: ident) => {
        impl $structname {
            #[allow(dead_code)]
            pub fn to_json(self: &Self) -> Vec<u8> {
                serde_json::to_vec(self).unwrap()
            }

            #[allow(dead_code)]
            pub fn from_json(json: &[u8]) -> Self {
                serde_json::from_slice(json).unwrap()
            }
        }
    };
}

#[derive(Serialize, Deserialize, Default, Hash, Eq, PartialEq, Clone, Debug)]
pub struct FunctionCall {
    pub name: String,
    pub args: String,
}
jsonize!(FunctionCall);

#[derive(Serialize, Deserialize, Default)]
pub struct Dag {
    #[serde(skip)]
    node_num: u64,

    #[serde(skip)]
    edge_num: u64,

    pub nodes: Vec<Rc<RefCell<Node>>>,
    // (prec, succ, edge_id)
    pub edges: HashSet<(u64, u64, u64)>,
}
jsonize!(Dag);

#[derive(Serialize, Deserialize, Default)]
pub struct DagRunner {
    pub nodes: Vec<Node>,
    // (prec, succ, edge_id)
    pub edges: HashSet<(u64, u64, u64)>,
}
jsonize!(DagRunner);

impl Dag {
    #[allow(dead_code)]
    pub fn new_node(self: &mut Self, name: &str, args: &str) -> Rc<RefCell<Node>> {
        self.node_num += 1;

        let node = Rc::new(RefCell::new(Node {
            id: self.node_num,
            func: FunctionCall {
                name: name.to_string(),
                args: args.to_string(),
            },
        }));

        self.nodes.push(node.clone());

        return node;
    }

    #[allow(dead_code)]
    pub fn add_dependency(self: &mut Self, prec: &Node, succ: &Node) {
        self.edge_num += 1;

        self.edges.insert((prec.id, succ.id, self.edge_num));
    }

    #[allow(dead_code)]
    pub fn add_independent(self: &mut Self, node: &Node) {
        self.edge_num += 1;

        self.edges.insert((0, node.id, self.edge_num));
    }
}

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Debug)]
pub struct Node {
    pub id: u64,
    pub func: FunctionCall,
}
jsonize!(Node);
