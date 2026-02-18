use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone)]
struct LayoutNode {
    order: usize,
    x: f32,
    y: f32,
    node_id: NodeIndex,
    parent_orders: Vec<usize>,
}

pub struct HierarchicalLayout {
    nodes: Vec<LayoutNode>,
    layers: Vec<Vec<usize>>,
    edges: Vec<(usize, usize)>,
    layer_spacing: f32,
    node_spacing: f32,
}

impl HierarchicalLayout {
    pub fn new(layer_spacing: f32, node_spacing: f32) -> Self {
        HierarchicalLayout {
            nodes: Vec::new(),
            layers: Vec::new(),
            edges: Vec::new(),
            layer_spacing,
            node_spacing,
        }
    }

    fn assign_layers(
        &mut self,
        schema: &DiGraph<String, ()>,
        root: NodeIndex,
    ) -> Result<(), String> {
        let mut layer_for_node: HashMap<NodeIndex, usize> = HashMap::new();
        let mut queue = VecDeque::new();
        let mut children_map: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();

        layer_for_node.insert(root, 0);
        queue.push_back(root);

        while let Some(node) = queue.pop_front() {
            let current_layer = *layer_for_node.get(&node).unwrap();

            for edge in schema.edges_directed(node, Direction::Outgoing) {
                let target = edge.target();
                children_map.entry(node).or_default().push(target);

                match layer_for_node.get(&target) {
                    Some(&existing_layer) => {
                        if existing_layer < current_layer + 1 {
                            layer_for_node.insert(target, current_layer + 1);
                            queue.push_back(target);
                        }
                    }
                    None => {
                        layer_for_node.insert(target, current_layer + 1);
                        queue.push_back(target);
                    }
                }
            }
        }

        if layer_for_node.len() != schema.node_count() {
            let max_layer = *layer_for_node.values().max().unwrap_or(&0);
            for node in schema.node_indices() {
                layer_for_node.entry(node).or_insert_with(|| max_layer + 1);
            }
        }

        let max_layer = *layer_for_node.values().max().unwrap();
        self.layers = vec![Vec::new(); max_layer + 1];

        let mut node_to_idx = HashMap::new();

        for (node_idx, &layer) in &layer_for_node {
            let layout_idx = self.nodes.len();

            let mut parents = Vec::new();
            for edge in schema.edges_directed(*node_idx, Direction::Incoming) {
                if let Some(&parent_idx) = node_to_idx.get(&edge.source()) {
                    parents.push(parent_idx);
                }
            }

            self.nodes.push(LayoutNode {
                order: 0,
                x: 0.0,
                y: layer as f32 * self.layer_spacing,
                node_id: *node_idx,
                parent_orders: Vec::new(),
            });

            node_to_idx.insert(*node_idx, layout_idx);
            self.layers[layer].push(layout_idx);
        }

        for i in 0..self.nodes.len() {
            let node_idx = self.nodes[i].node_id;
            let mut parents = Vec::new();

            for edge in schema.edges_directed(node_idx, Direction::Incoming) {
                if let Some(&parent_idx) = node_to_idx.get(&edge.source()) {
                    parents.push(parent_idx);
                }
            }
            self.nodes[i].parent_orders = parents;
        }

        for edge in schema.edge_indices() {
            let (source, target) = schema.edge_endpoints(edge).unwrap();
            if let (Some(&src_idx), Some(&tgt_idx)) =
                (node_to_idx.get(&source), node_to_idx.get(&target))
            {
                self.edges.push((src_idx, tgt_idx));
            }
        }

        self.initial_layer_sorting();

        Ok(())
    }

    fn initial_layer_sorting(&mut self) {
        for layer in (0..self.layers.len()).rev() {
            let layer_nodes = self.layers[layer].clone();

            let mut nodes_with_weight: Vec<(usize, f32)> = layer_nodes
                .iter()
                .map(|&node_idx| {
                    let node = &self.nodes[node_idx];

                    if node.parent_orders.is_empty() {
                        (node_idx, 0.0)
                    } else {
                        let parent_indices = &node.parent_orders;
                        let parent_positions: Vec<usize> = parent_indices
                            .iter()
                            .map(|&p_idx| self.nodes[p_idx].order)
                            .collect();

                        let avg_parent = parent_positions.iter().sum::<usize>() as f32
                            / parent_positions.len() as f32;
                        (node_idx, avg_parent)
                    }
                })
                .collect();

            nodes_with_weight.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            self.layers[layer] = nodes_with_weight.into_iter().map(|(idx, _)| idx).collect();

            for (order, &node_idx) in self.layers[layer].iter().enumerate() {
                self.nodes[node_idx].order = order;
            }
        }

        for layer in 1..self.layers.len() {
            self.sort_layer_by_parents(layer);
        }
    }

    fn sort_layer_by_parents(&mut self, layer_idx: usize) {
        let layer_nodes = self.layers[layer_idx].clone();

        let mut nodes_with_avg: Vec<(usize, f32)> = layer_nodes
            .iter()
            .map(|&node_idx| {
                let node = &self.nodes[node_idx];

                if node.parent_orders.is_empty() {
                    (node_idx, node.order as f32)
                } else {
                    let avg_parent = node
                        .parent_orders
                        .iter()
                        .map(|&p_idx| self.nodes[p_idx].order as f32)
                        .sum::<f32>()
                        / node.parent_orders.len() as f32;
                    (node_idx, avg_parent)
                }
            })
            .collect();

        nodes_with_avg.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        self.layers[layer_idx] = nodes_with_avg.into_iter().map(|(idx, _)| idx).collect();

        for (order, &node_idx) in self.layers[layer_idx].iter().enumerate() {
            self.nodes[node_idx].order = order;
        }
    }

    fn assign_x_coordinates(&mut self) {
        for layer_nodes in &self.layers {
            let count = layer_nodes.len() as f32;
            if count <= 1.0 {
                if count == 1.0 {
                    self.nodes[layer_nodes[0]].x = 0.0;
                }
                continue;
            }

            let total_width = (count - 1.0) * self.node_spacing;
            let start_x = -total_width / 2.0;

            for (i, &node_idx) in layer_nodes.iter().enumerate() {
                self.nodes[node_idx].x = start_x + i as f32 * self.node_spacing;
            }
        }

        for iteration in 0..1 {
            let mut current_x: Vec<f32> = self.nodes.iter().map(|n| n.x).collect();

            for layer_idx in 1..self.layers.len() {
                for &node_idx in &self.layers[layer_idx] {
                    let mut parent_x_sum = 0.0;
                    let mut parent_count = 0;

                    for &(src, tgt) in &self.edges {
                        if tgt == node_idx {
                            parent_x_sum += current_x[src];
                            parent_count += 1;
                        }
                    }

                    if parent_count > 0 {
                        let avg_parent_x = parent_x_sum / parent_count as f32;

                        let parent_influence = 0.7 * (1.0 - iteration as f32 * 0.1);
                        current_x[node_idx] = self.nodes[node_idx].x * (1.0 - parent_influence)
                            + avg_parent_x * parent_influence;
                    }
                }
            }

            for layer_idx in (0..self.layers.len() - 1).rev() {
                for &node_idx in &self.layers[layer_idx] {
                    let mut child_x_sum = 0.0;
                    let mut child_count = 0;

                    for &(src, tgt) in &self.edges {
                        if src == node_idx {
                            child_x_sum += current_x[tgt];
                            child_count += 1;
                        }
                    }

                    if child_count > 0 {
                        let avg_child_x = child_x_sum / child_count as f32;

                        let child_influence = 0.5 * (1.0 - iteration as f32 * 0.1);
                        current_x[node_idx] = current_x[node_idx] * (1.0 - child_influence)
                            + avg_child_x * child_influence;
                    }
                }
            }

            for (i, node) in self.nodes.iter_mut().enumerate() {
                node.x = current_x[i];
            }

            self.enforce_minimum_distance();
        }
    }

    fn enforce_minimum_distance(&mut self) {
        for layer_nodes in &self.layers {
            if layer_nodes.len() <= 1 {
                continue;
            }

            for _ in 0..1 {
                let mut positions: Vec<(usize, f32)> = layer_nodes
                    .iter()
                    .map(|&idx| (idx, self.nodes[idx].x))
                    .collect();

                positions.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

                let mut adjusted = false;

                for i in 1..positions.len() {
                    let (prev_idx, prev_x) = positions[i - 1];
                    let (curr_idx, curr_x) = positions[i];

                    let distance = curr_x - prev_x;
                    if distance < self.node_spacing {
                        let needed_space = self.node_spacing - distance;

                        let push_left = needed_space / 2.0;
                        let push_right = needed_space - push_left;

                        self.nodes[prev_idx].x -= push_left;
                        self.nodes[curr_idx].x += push_right;

                        positions[i - 1].1 = self.nodes[prev_idx].x;
                        positions[i].1 = self.nodes[curr_idx].x;

                        adjusted = true;
                    }
                }

                if !adjusted {
                    break;
                }
            }
        }

        self.center_layers();
    }

    fn center_layers(&mut self) {
        for layer_nodes in &self.layers {
            if layer_nodes.is_empty() {
                continue;
            }

            let min_x = layer_nodes
                .iter()
                .map(|&idx| self.nodes[idx].x)
                .fold(f32::INFINITY, |a, b| a.min(b));

            let max_x = layer_nodes
                .iter()
                .map(|&idx| self.nodes[idx].x)
                .fold(f32::NEG_INFINITY, |a, b| a.max(b));

            let center = (min_x + max_x) / 2.0;

            for &node_idx in layer_nodes {
                self.nodes[node_idx].x -= center;
            }
        }
    }

    fn assign_x_coordinates_strict(&mut self) {
        self.assign_x_coordinates();

        for layer_nodes in &self.layers {
            if layer_nodes.len() <= 1 {
                continue;
            }

            let mut desired_positions: Vec<(usize, f32)> = layer_nodes
                .iter()
                .map(|&idx| (idx, self.nodes[idx].x))
                .collect();

            desired_positions.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let mut final_positions = Vec::with_capacity(desired_positions.len());

            final_positions.push((desired_positions[0].0, desired_positions[0].1));

            for i in 1..desired_positions.len() {
                let (node_idx, desired_x) = desired_positions[i];
                let (_, prev_final_x) = final_positions[i - 1];

                let min_allowed_x = prev_final_x + self.node_spacing;
                let new_x = desired_x.max(min_allowed_x);

                final_positions.push((node_idx, new_x));
            }

            for (node_idx, new_x) in final_positions {
                self.nodes[node_idx].x = new_x;
            }

            let min_x = self.nodes[layer_nodes[0]].x;
            let max_x = self.nodes[layer_nodes[layer_nodes.len() - 1]].x;
            let center = (min_x + max_x) / 2.0;

            for &node_idx in layer_nodes {
                self.nodes[node_idx].x -= center;
            }
        }
    }

    fn get_neighbor_orders(&self, node_idx: usize, direction: Direction) -> Vec<usize> {
        let mut orders = Vec::new();

        for &(src, tgt) in &self.edges {
            match direction {
                Direction::Incoming => {
                    if tgt == node_idx {
                        orders.push(self.nodes[src].order);
                    }
                }
                Direction::Outgoing => {
                    if src == node_idx {
                        orders.push(self.nodes[tgt].order);
                    }
                }
            }
        }

        orders.sort();
        orders
    }

    fn median(orders: &[usize]) -> f32 {
        if orders.is_empty() {
            return 0.0;
        }
        let mid = orders.len() / 2;
        if orders.len().is_multiple_of(2) {
            (orders[mid - 1] + orders[mid]) as f32 / 2.0
        } else {
            orders[mid] as f32
        }
    }

    fn minimize_crossings(&mut self) {
        for _ in 0..5 {
            for layer in 1..self.layers.len() {
                self.sort_layer_by_median(layer, Direction::Incoming);
            }

            for layer in (0..self.layers.len() - 1).rev() {
                self.sort_layer_by_median(layer, Direction::Outgoing);
            }
        }

        for layer_nodes in self.layers.iter() {
            for (order, &node_idx) in layer_nodes.iter().enumerate() {
                self.nodes[node_idx].order = order;
            }
        }
    }

    fn sort_layer_by_median(&mut self, layer_idx: usize, direction: Direction) {
        let layer_nodes = self.layers[layer_idx].clone();

        let mut node_medians: Vec<(usize, f32)> = layer_nodes
            .iter()
            .map(|&node_idx| {
                let neighbor_orders = self.get_neighbor_orders(node_idx, direction);
                let median = if neighbor_orders.is_empty() {
                    self.nodes[node_idx].order as f32
                } else {
                    Self::median(&neighbor_orders)
                };
                (node_idx, median)
            })
            .collect();

        node_medians.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        self.layers[layer_idx] = node_medians
            .into_iter()
            .map(|(node_idx, _)| node_idx)
            .collect();
    }

    pub fn get_node_positions(&self) -> Vec<(NodeIndex, f32, f32)> {
        self.nodes.iter().map(|n| (n.node_id, n.x, n.y)).collect()
    }

    pub fn layout(
        &mut self,
        schema: &DiGraph<String, ()>,
        root: NodeIndex,
    ) -> Result<Vec<(NodeIndex, f32, f32)>, String> {
        self.assign_layers(schema, root)?;

        println!(
            "Слоев: {}, узлов: {}, ребер: {}",
            self.layers.len(),
            self.nodes.len(),
            self.edges.len()
        );

        self.minimize_crossings();

        self.assign_x_coordinates_strict();

        Ok(self.get_node_positions())
    }
}

#[cfg(test)]
mod layout_tests {
    use std::time::Instant;

    use super::*;
    use rand::random_range;
    use svg::node::element::path::Data;
    use svg::node::element::{Line, Marker, Path, Rectangle, Text};
    use svg::{Document, Node};

    #[test]
    #[ignore = "Визуальный тест"]
    fn test_layout() {
        let node_width = 40.0;
        let node_height = 30.0;

        let mut schema = DiGraph::<String, ()>::new();

        let root = schema.add_node("Root".to_string());
        let mut prev_level = vec![root];
        println!("Добавлен узел: {}", root.index());

        let mut level = 1;
        while schema.node_count() < 100 {
            let mut current_level = Vec::new();
            while current_level.is_empty() {
                for &parent in &prev_level {
                    for i in 0..random_range(0..3) {
                        let child = schema.add_node(format!("Node_{}_{}", level, i));
                        schema.add_edge(parent, child, ());

                        let rand_parent = random_range(0..=prev_level.len());
                        if rand_parent > 0 {
                            schema.add_edge(prev_level[rand_parent - 1], child, ());
                        }

                        current_level.push(child);
                    }
                }
            }
            level += 1;
            prev_level = current_level;
        }

        println!("Создан граф с {} узлами", schema.node_count());
        let layout_start = Instant::now();
        let mut layout = HierarchicalLayout::new(80.0, 80.0);
        let positions = layout.layout(&schema, root).unwrap();

        println!(
            "Positions calculated in {}ms",
            layout_start.elapsed().as_millis()
        );

        let colors = [
            "red",
            "blue",
            "green",
            "yellow",
            "orange",
            "purple",
            "magenta",
            "cyan",
            "olivedrab",
            "crimson",
        ];

        let mut bounds = (0.0, 0.0, 0.0, 0.0);
        let mut svg_objects: Vec<Box<dyn Node>> = vec![];

        for (node_idx, x, y) in positions.clone() {
            if x < bounds.0 {
                bounds.0 = x;
            }
            if y < bounds.1 {
                bounds.1 = y;
            }
            if x > bounds.2 {
                bounds.2 = x;
            }
            if y > bounds.3 {
                bounds.3 = y;
            }

            let color = colors[node_idx.index() % colors.len()];

            svg_objects.push(Box::new(
                Rectangle::new()
                    .set("x", x)
                    .set("y", y)
                    .set("width", node_width)
                    .set("height", node_height)
                    .set("stroke", color)
                    .set("fill", "none"),
            ));

            svg_objects.push(Box::new(
                Text::new(format!("{}", node_idx.index()))
                    .set("x", x)
                    .set("y", y + 25.0)
                    .set("font-family", "Courier New")
                    .set("font-size", "16")
                    .set("fill", color),
            ));

            for edge in schema.edges_directed(node_idx, Direction::Incoming) {
                let n = positions
                    .iter()
                    .find(|i| i.0.index() == edge.source().index())
                    .unwrap();

                svg_objects.push(Box::new(
                    Line::new()
                        .set("stroke", color)
                        .set("stroke-width", 1)
                        .set("x1", x + node_width / 2.0)
                        .set("y1", y)
                        .set("x2", n.1 + node_width / 2.0)
                        .set("y2", n.2 + node_height)
                        .set("marker-end", "url(#Arrow)"),
                ));
            }
        }

        bounds.2 = bounds.2 + node_width + bounds.0.abs();
        bounds.3 = bounds.3 + node_height + bounds.1.abs();

        println!("Bounds {:?}", bounds);

        let mut document = Document::new()
            .set("width", bounds.2)
            .set("height", bounds.3)
            .set("viewBox", bounds)
            .add(
                Rectangle::new()
                    .set("x", bounds.0)
                    .set("y", bounds.1)
                    .set("width", bounds.2)
                    .set("height", bounds.3)
                    .set("fill", "#1E1E1E"),
            );

        document = document.add(
            Marker::new()
                .set("id", "Arrow")
                .set("viewBox", (0.0, 0.0, 10.0, 10.0))
                .set("refX", "8")
                .set("refY", "5")
                .set("markerUnits", "strokeWidth")
                .set("markerWidth", "4")
                .set("markerHeight", "3")
                .set("orient", "auto")
                .add(
                    Path::new().set("fill", "context-stroke").set(
                        "d",
                        Data::new()
                            .move_to((0.0, 0.0))
                            .line_to((10.0, 5.0))
                            .line_to((0.0, 10.0))
                            .close(),
                    ),
                ),
        );

        for obj in svg_objects {
            document = document.add(obj);
        }

        svg::save("image.svg", &document).unwrap();
    }
}
