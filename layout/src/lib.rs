use std::{collections::HashMap, fmt::Debug, hash::Hash};

use ica_core::{NodeId, Schema};

pub struct Position {
    pub x: f64,
    pub y: f64,
}

#[derive(Clone)]
pub struct LayoutSettings {
    pub space_between_nodes: f64,
    pub node_width: f64,
    pub node_height: f64,
}

pub trait Layout<T: Clone + Default + Debug + Hash + Eq> {
    fn layout(&self, settings: LayoutSettings) -> HashMap<NodeId<T>, Position>;
}

impl<SA: Default, NA: Default, EA: Default, T: Clone + Default + Debug + Hash + Eq> Layout<T>
    for Schema<SA, NA, EA, T>
{
    fn layout(&self, settings: LayoutSettings) -> HashMap<NodeId<T>, Position> {
        let mut positions = HashMap::new();
        let mut path = self.get_full_path().unwrap();

        if path.is_empty() {
            return positions;
        }

        path.reverse();

        // Словарь для хранения уровня каждого узла (начиная с 0 для корня)
        let mut node_levels: HashMap<NodeId<T>, usize> = HashMap::new();
        node_levels.insert(path.first().unwrap().clone(), 0);

        // Определяем уровни для всех узлов
        for node_id in &path {
            let current_level = *node_levels.get(node_id).unwrap_or(&0);

            for edge in self.incoming_edges(node_id) {
                let child_id = edge.from.clone();
                node_levels.insert(child_id, current_level + 1);
            }
        }

        // Группируем узлы по уровням
        let mut nodes_by_level: HashMap<usize, Vec<NodeId<T>>> = HashMap::new();
        for (node_id, level) in &node_levels {
            nodes_by_level
                .entry(*level)
                .or_default()
                .push(node_id.clone());
        }

        // Вычисляем позиции для каждого уровня
        for (level, nodes) in nodes_by_level {
            // Количество узлов на текущем уровне
            let node_count = nodes.len();

            // Общая ширина, занимаемая узлами на этом уровне
            let total_width = node_count as f64 * settings.node_width
                + (node_count as f64 - 1.0) * settings.space_between_nodes;

            // Начальная X координата для центрирования уровня
            let start_x = -total_width / 2.0 + settings.node_width / 2.0;

            // Y координата для этого уровня
            let y = level as f64 * (settings.node_height + settings.space_between_nodes);

            // Сортируем узлы для предсказуемого порядка (опционально)
            let mut sorted_nodes = nodes;
            sorted_nodes.sort_by_key(|id| format!("{:?}", id));

            // Распределяем узлы горизонтально
            for (i, node_id) in sorted_nodes.into_iter().enumerate() {
                let x = start_x + i as f64 * (settings.node_width + settings.space_between_nodes);
                positions.insert(node_id, Position { x, y });
            }
        }

        positions
    }
}

#[cfg(test)]
mod tests_layout {
    use super::*;
    use ica_core::{Attributes, EdgeId, NodeId, Schema};
    use rand::random_range;
    use svg::node::element::path::Data;
    use svg::node::element::{Line, Marker, Path, Rectangle, Text};
    use svg::{Document, Node};

    fn generate_schema_test_data(
        nodes_count: usize,
    ) -> Schema<Attributes, Attributes, Attributes, u64> {
        let mut g: Schema<Attributes, Attributes, Attributes, u64> = Schema::new(Attributes::new());
        let mut node_counter: u64 = 0;
        let mut edge_counter: u64 = 0;

        let mut next_node_id = || -> NodeId<u64> {
            node_counter += 1;
            NodeId(node_counter)
        };

        let mut next_edge_id = || -> EdgeId<u64> {
            edge_counter += 1;
            EdgeId(edge_counter)
        };

        let root = next_node_id();
        g.insert_node(root, Attributes::new()).unwrap();
        let mut prev_level = vec![root];

        while g.node_count() < nodes_count {
            let mut current_level = Vec::new();
            while current_level.is_empty() {
                for &parent in &prev_level {
                    for _ in 0..random_range(0..3) {
                        let child_id = next_node_id();
                        g.insert_node(child_id, Attributes::new()).unwrap();
                        let _ = g.insert_edge(next_edge_id(), child_id, parent, Attributes::new());

                        let rand_parent = random_range(0..=prev_level.len());
                        if rand_parent > 0 && prev_level[rand_parent - 1].0 != parent.0 {
                            let _ = g.insert_edge(
                                next_edge_id(),
                                child_id,
                                prev_level[rand_parent - 1],
                                Attributes::new(),
                            );
                        }

                        current_level.push(child_id);
                    }
                }
            }
            prev_level = current_level;
        }

        g
    }

    #[test]
    fn test_layout() {
        let mut g: Schema<Attributes, Attributes, Attributes, u64> = Schema::new(Attributes::new());
        let mut counter: u64 = 0;

        // nodes
        let root = {
            counter += 1;
            let id = NodeId(counter);
            let _ = g.insert_node(id, Attributes::new());
            id
        };
        let node2 = {
            counter += 1;
            let id = NodeId(counter);
            let _ = g.insert_node(id, Attributes::new());
            id
        };
        let node3 = {
            counter += 1;
            let id = NodeId(counter);
            let _ = g.insert_node(id, Attributes::new());
            id
        };

        // edges
        {
            counter += 1;
            let id = EdgeId(counter);
            let _ = g.insert_edge(id, node2, root, Attributes::new());
        }
        {
            counter += 1;
            let id = EdgeId(counter);
            let _ = g.insert_edge(id, node3, root, Attributes::new());
        }

        let settings = LayoutSettings {
            space_between_nodes: 60.0,
            node_width: 80.0,
            node_height: 40.0,
        };

        let positions = g.layout(settings);

        assert_eq!(positions.get(&root).unwrap().x, 0.0);
        assert_eq!(positions.get(&root).unwrap().y, 0.0);

        assert_eq!(positions.get(&node2).unwrap().x, -70.0);
        assert_eq!(positions.get(&node2).unwrap().y, 100.0);

        assert_eq!(positions.get(&node3).unwrap().x, 70.0);
        assert_eq!(positions.get(&node3).unwrap().y, 100.0);
    }

    #[test]
    #[ignore = "Визуальный тест"]
    fn test_layout_random() {
        let g = generate_schema_test_data(10000);

        println!("Создан граф с {} узлами", g.node_count());

        let settings = LayoutSettings {
            space_between_nodes: 60.0,
            node_width: 80.0,
            node_height: 40.0,
        };

        let positions = g.layout(settings.clone());
        println!("Сохраняем в image.svg ...");

        // DRAWING

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

        for (node_id, pos) in &positions {
            if pos.x < bounds.0 {
                bounds.0 = pos.x;
            }
            if pos.y < bounds.1 {
                bounds.1 = pos.y;
            }
            if pos.x > bounds.2 {
                bounds.2 = pos.x;
            }
            if pos.y > bounds.3 {
                bounds.3 = pos.y;
            }

            let color_idx = node_id.0 as usize % colors.len();
            let color = colors[color_idx];

            svg_objects.push(Box::new(
                Rectangle::new()
                    .set("x", pos.x)
                    .set("y", pos.y)
                    .set("width", settings.node_width)
                    .set("height", settings.node_height)
                    .set("stroke", color)
                    .set("fill", "none"),
            ));

            svg_objects.push(Box::new(
                Text::new(format!("{}", node_id.0))
                    .set("x", pos.x)
                    .set("y", pos.y + 25.0)
                    .set("font-family", "Courier New")
                    .set("font-size", "16")
                    .set("fill", color),
            ));

            for edge in g.outgoing_edges(node_id) {
                let n = &positions.get(&edge.to).unwrap();

                svg_objects.push(Box::new(
                    Line::new()
                        .set("stroke", color)
                        .set("stroke-width", 1)
                        .set("x1", pos.x + settings.node_width / 2.0)
                        .set("y1", pos.y)
                        .set("x2", n.x + settings.node_width / 2.0)
                        .set("y2", n.y + settings.node_height)
                        .set("marker-end", "url(#Arrow)"),
                ));
            }
        }

        bounds.2 = bounds.2 + settings.node_width + bounds.0.abs();
        bounds.3 = bounds.3 + settings.node_height + bounds.1.abs();

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
