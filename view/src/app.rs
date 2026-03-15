use std::{collections::HashMap, sync::Arc};

use egui::{Align2, Color32, Stroke};
use tokio::sync::RwLock;

use crate::client::{
    self, GetAttrValue,
    schema_contracts::{Edge, Node},
};

pub struct StateApp {
    nodes: Arc<RwLock<HashMap<String, HashMap<String, Node>>>>,
    edges: Arc<RwLock<HashMap<String, HashMap<String, Edge>>>>,
    schemas: Arc<RwLock<HashMap<String, String>>>,
    schemas_active: Arc<RwLock<HashMap<String, bool>>>,
    node_state: Arc<RwLock<HashMap<String, Color32>>>,
}

pub struct TemplateApp {
    app_state: Arc<StateApp>,
    node_stroke_color: HashMap<String, Color32>,
    ctx: Arc<egui::Context>,
}

impl TemplateApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let s = Self {
            app_state: Arc::new(StateApp {
                schemas: Arc::new(RwLock::new(HashMap::new())),
                schemas_active: Arc::new(RwLock::new(HashMap::new())),
                edges: Arc::new(RwLock::new(HashMap::new())),
                nodes: Arc::new(RwLock::new(HashMap::new())),
                node_state: Arc::new(RwLock::new(HashMap::new())),
            }),
            node_stroke_color: HashMap::new(),
            ctx: Arc::new(cc.egui_ctx.clone()),
        };

        s.read_state();

        s
    }

    fn read_state(&self) {
        let app_state = self.app_state.clone();
        let ctx = self.ctx.clone();

        wasm_bindgen_futures::spawn_local(async move {
            loop {
                gloo_timers::future::TimeoutFuture::new(1_000).await;
                let states = match client::get_states().await {
                    Ok(states) => states,
                    Err(e) => {
                        eprintln!("Error: {:?}", e);
                        continue;
                    }
                };

                for (node_id, state) in states {
                    let color = if state == 0.0 {
                        Color32::TRANSPARENT
                    } else if state < 0.25 {
                        Color32::DARK_GREEN
                    } else if state < 0.5 {
                        Color32::GREEN
                    } else if state < 0.75 {
                        Color32::YELLOW
                    } else {
                        Color32::RED
                    };

                    let mut node_state = app_state.node_state.write().await;
                    node_state.insert(node_id.clone(), color);
                }

                ctx.request_repaint();
            }
        });
    }

    fn fetch_schemas(&self) {
        let state = self.app_state.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let resp = client::list_schemas().await;
            let mut schemas = state.schemas.write().await;
            let mut schemas_active = state.schemas_active.write().await;
            let items = resp.unwrap();

            for i in items.clone() {
                schemas.insert(i.0.clone(), i.1);
                schemas_active.insert(i.0, false);
            }
        });
    }

    fn load_schema(&self, schema_id: String) {
        let state = self.app_state.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let (nodes, edges) = client::get_schema(schema_id.clone()).await.unwrap();
            state.nodes.write().await.insert(schema_id.clone(), nodes);
            state.edges.write().await.insert(schema_id, edges);
        });
    }
}

impl eframe::App for TemplateApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::MenuBar::new().ui(ui, |ui| {
                if ui.button("Обновить").clicked() {
                    self.fetch_schemas();
                }
            });
        });

        let app_state = self.app_state.clone();
        egui::SidePanel::new(egui::panel::Side::Left, "left_menu")
            .min_width(200.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("Схемы");
                ui.vertical(|ui| {
                    let schemas = app_state.schemas.blocking_read().clone();
                    let mut schemas_active = app_state.schemas_active.blocking_write();
                    for (key, value) in schemas {
                        let m = schemas_active.get_mut(&key).unwrap();
                        ui.checkbox(m, value);
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            let mut schemas_active = app_state.schemas_active.blocking_write();
            let nodes_stroke_colors: Vec<Color32> = vec![
                Color32::from_rgb(245, 255, 250),
                Color32::from_rgb(255, 209, 220),
                Color32::from_rgb(255, 218, 185),
                Color32::from_rgb(175, 238, 238),
                Color32::from_rgb(230, 230, 250),
                Color32::from_rgb(208, 240, 192),
                Color32::from_rgb(176, 224, 230),
                Color32::from_rgb(253, 253, 150),
                Color32::from_rgb(230, 190, 255),
                Color32::from_rgb(255, 160, 160),
                Color32::from_rgb(164, 211, 238),
                Color32::from_rgb(200, 175, 215),
                Color32::from_rgb(255, 253, 208),
                Color32::from_rgb(230, 205, 185),
                Color32::from_rgb(224, 176, 255),
            ];
            for (key, value) in schemas_active.iter_mut() {
                if *value && !app_state.nodes.blocking_read().contains_key(key) {
                    self.load_schema(key.clone());
                }

                egui::Window::new(key.clone())
                    .default_width(320.0)
                    .default_height(480.0)
                    .open(value)
                    .resizable([true, true])
                    .scroll(true)
                    .constrain_to(ui.available_rect_before_wrap())
                    .show(ctx, |ui| {
                        egui::containers::Frame::canvas(ui.style())
                            .stroke(Stroke::NONE)
                            .fill(Color32::TRANSPARENT)
                            .show(ui, |ui| {
                                let available = ui.available_rect_before_wrap();
                                let rect = available.shrink(5.0);
                                ui.painter().rect_filled(
                                    rect,
                                    2.0,
                                    egui::Color32::from_rgb(14, 14, 14),
                                );

                                let node_state = app_state.node_state.blocking_read();

                                if let Some(nodes) = app_state.nodes.blocking_read().get(key) {
                                    for (idx, node) in nodes.iter().enumerate() {
                                        let color = match self.node_stroke_color.get(node.0) {
                                            Some(color) => *color,
                                            None => {
                                                let c = nodes_stroke_colors
                                                    [idx % nodes_stroke_colors.len()];
                                                self.node_stroke_color.insert(node.0.clone(), c);
                                                c
                                            }
                                        };

                                        let x =
                                            node.1.attributes.get_float("x").unwrap_or(0.0) as f32;
                                        let y =
                                            node.1.attributes.get_float("y").unwrap_or(0.0) as f32;

                                        let rect = egui::Rect::from_min_size(
                                            egui::pos2(
                                                rect.center_top().x - 40.0,
                                                rect.center_top().y + 20.0,
                                            ) + egui::vec2(x, y),
                                            egui::vec2(80.0, 40.0),
                                        );

                                        ui.painter().rect_stroke(
                                            rect,
                                            5.0,
                                            egui::Stroke::new(1.0, color),
                                            egui::StrokeKind::Outside,
                                        );

                                        let node_state = *node_state
                                            .get(node.0)
                                            .unwrap_or(&Color32::TRANSPARENT);

                                        ui.painter().rect_filled(rect, 5.0, node_state);

                                        ui.painter().text(
                                            rect.center(),
                                            Align2::CENTER_CENTER,
                                            node.0.clone(),
                                            egui::FontId::proportional(10.0),
                                            if node_state == Color32::TRANSPARENT {
                                                Color32::WHITE
                                            } else {
                                                Color32::BLACK
                                            },
                                        );
                                    }
                                }

                                if let Some(edges) = app_state.edges.blocking_read().get(key) {
                                    let nodes = app_state.nodes.blocking_read();
                                    let schema_nodes = nodes.get(key).unwrap();

                                    for edge in edges {
                                        let arrow_start = if let Some(from) =
                                            schema_nodes.get(&edge.1.from_id)
                                        {
                                            let from_x =
                                                from.attributes.get_float("x").unwrap_or(0.0)
                                                    as f32;
                                            let from_y =
                                                from.attributes.get_float("y").unwrap_or(0.0)
                                                    as f32;

                                            let color =
                                                match self.node_stroke_color.get(&edge.1.from_id) {
                                                    Some(color) => *color,
                                                    None => Color32::WHITE,
                                                };

                                            Some((
                                                egui::pos2(
                                                    from_x + rect.center_top().x,
                                                    from_y + rect.center_top().y + 20.0,
                                                ),
                                                color,
                                            ))
                                        } else {
                                            None
                                        };
                                        let arrow_end = if let Some(to) =
                                            schema_nodes.get(&edge.1.to_id)
                                        {
                                            let to_x =
                                                to.attributes.get_float("x").unwrap_or(0.0) as f32;
                                            let to_y =
                                                to.attributes.get_float("y").unwrap_or(0.0) as f32;

                                            Some(egui::pos2(
                                                to_x + rect.center_top().x,
                                                to_y + rect.center_top().y + 60.0,
                                            ))
                                        } else {
                                            None
                                        };

                                        if let Some((start, color)) = arrow_start
                                            && let Some(end) = arrow_end
                                        {
                                            ui.painter()
                                                .line(vec![start, end], Stroke::new(1.0, color));
                                            ui.painter().circle(
                                                end,
                                                5.0,
                                                Color32::WHITE,
                                                Stroke::NONE,
                                            );
                                        }
                                    }
                                }
                            });
                    });
            }
        });
    }
}
