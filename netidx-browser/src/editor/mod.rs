mod source_inspector;
mod util;
use super::{
    util::err_modal, FromGui, Target, ToGui, WidgetCtx, WidgetPath, DEFAULT_PROPS,
};
use glib::{clone, idle_add_local, prelude::*, subclass::prelude::*, GString};
use gtk::{self, prelude::*};
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_protocols::view;
use source_inspector::SourceInspector;
use std::{
    boxed,
    cell::{Cell, RefCell},
    rc::Rc,
    result,
};
use util::{parse_entry, TwoColGrid};

type OnChange = Rc<dyn Fn()>;

#[derive(Clone, Debug)]
struct Table {
    root: gtk::Box,
    spec: Rc<RefCell<Path>>,
}

impl Table {
    fn new(on_change: OnChange, path: Path) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let label = gtk::Label::new(Some("Path:"));
        let entry = gtk::Entry::new();
        root.pack_start(&label, false, false, 0);
        root.pack_start(&entry, true, true, 0);
        let spec = Rc::new(RefCell::new(path));
        entry.set_text(&**spec.borrow());
        entry.connect_activate(clone!(@strong spec => move |e| {
            *spec.borrow_mut() = Path::from(String::from(&*e.get_text()));
            on_change()
        }));
        Table { root, spec }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Table(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

type DbgSrc = Rc<RefCell<Option<(gtk::Window, SourceInspector)>>>;

fn source(
    ctx: &WidgetCtx,
    txt: &str,
    init: &view::Source,
    on_change: impl Fn(view::Source) + 'static,
) -> (gtk::Label, gtk::Box, DbgSrc) {
    let on_change = Rc::new(on_change);
    let source = Rc::new(RefCell::new(init.clone()));
    let inspector: Rc<RefCell<Option<(gtk::Window, SourceInspector)>>> =
        Rc::new(RefCell::new(None));
    let lbl = gtk::Label::new(Some(txt));
    let ibox = gtk::Box::new(gtk::Orientation::Horizontal, 0);
    let entry = gtk::Entry::new();
    let inspect = gtk::ToggleButton::new();
    let inspect_icon = gtk::Image::from_icon_name(
        Some("preferences-system"),
        gtk::IconSize::SmallToolbar,
    );
    inspect.set_image(Some(&inspect_icon));
    ibox.pack_start(&entry, true, true, 0);
    ibox.pack_end(&inspect, false, false, 0);
    entry.set_text(&source.borrow().to_string());
    entry.connect_activate(clone!(
        @strong on_change, @strong source, @weak inspect, @weak ibox => move |e| {
        match e.get_text().parse::<view::Source>() {
            Err(e) => err_modal(&ibox, &format!("parse error: {}", e)),
            Ok(s) => {
                inspect.set_active(false);
                *source.borrow_mut() = s.clone();
                on_change(s);
            }
        }
    }));
    inspect.connect_toggled(clone!(
    @strong ctx,
    @strong on_change,
    @strong inspector,
    @strong source,
    @weak entry => move |b| {
        if !b.get_active() {
            if let Some((w, _)) = inspector.borrow_mut().take() {
                w.close()
            }
        } else {
            let w = gtk::Window::new(gtk::WindowType::Toplevel);
            w.set_default_size(640, 480);
            let on_change = {
                let on_change = on_change.clone();
                let entry = entry.clone();
                let source = source.clone();
                move |s: view::Source| {
                    entry.set_text(&s.to_string());
                    *source.borrow_mut() = s.clone();
                    on_change(s)
                }
            };
            let si = SourceInspector::new(ctx.clone(), on_change, source.borrow().clone());
            w.add(si.root());
            si.root().set_property_margin(5);
            w.connect_delete_event(clone!(@strong inspector, @strong b => move |_, _| {
                *inspector.borrow_mut() = None;
                b.set_active(false);
                Inhibit(false)
            }));
            w.show_all();
            *inspector.borrow_mut() = Some((w, si));
        }
    }));
    (lbl, ibox, inspector)
}

#[derive(Clone, Debug)]
struct Action {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Action>>,
    source: DbgSrc,
}

impl Action {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Action) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (srclbl, srcent, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((srclbl, srcent));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        Action { root, spec, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Action(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<view::Source>>,
    source: DbgSrc,
}

impl Label {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Source) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &*spec.borrow(),
            clone!(@strong spec => move |s| {
                *spec.borrow_mut() = s;
                on_change()
            }),
        );
        root.pack_start(&l, false, false, 0);
        root.pack_start(&e, true, true, 0);
        Label { root, spec, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Label(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    enabled_source: DbgSrc,
    label_source: DbgSrc,
    source: DbgSrc,
}

impl Button {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Button) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, label_source) = source(
            ctx,
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        Button { root, spec, enabled_source, label_source, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Button(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.label_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
    enabled_source: DbgSrc,
    source: DbgSrc,
}

impl Toggle {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Toggle) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change();
            }),
        ));
        Toggle { root, spec, enabled_source, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Toggle(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
    enabled_source: DbgSrc,
    choices_source: DbgSrc,
    source: DbgSrc,
}

impl Selector {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Selector) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, choices_source) = source(
            ctx,
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        Selector { root, spec, enabled_source, choices_source, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Selector(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.choices_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    enabled_source: DbgSrc,
    visible_source: DbgSrc,
    source: DbgSrc,
}

impl Entry {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Entry) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, visible_source) = source(
            ctx,
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        Entry { root, spec, enabled_source, visible_source, source }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Entry(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.visible_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Series {
    title: DbgSrc,
    x: DbgSrc,
    y: DbgSrc,
}

#[derive(Clone, Debug)]
struct LinePlot {
    root: gtk::Box,
    spec: Rc<RefCell<view::LinePlot>>,
    title: DbgSrc,
    x_label: DbgSrc,
    y_label: DbgSrc,
    timeseries: DbgSrc,
    keep_points: DbgSrc,
    series: Rc<RefCell<Vec<Series>>>,
}

impl LinePlot {
    fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::LinePlot) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let mut common = TwoColGrid::new();
        root.pack_start(common.root(), false, false, 0);
        let (l, e, title) = source(
            ctx,
            "Title:",
            &spec.borrow().title,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().title = s;
                on_change()
            }),
        );
        common.add((l, e));
        let (l, e, x_label) = source(
            ctx,
            "X Axis Label:",
            &spec.borrow().x_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_label = s;
                on_change()
            }),
        );
        common.add((l, e));
        let (l, e, y_label) = source(
            ctx,
            "Y Axis Label:",
            &spec.borrow().y_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_label = s;
                on_change()
            }),
        );
        common.add((l, e));
        let (l, e, timeseries) = source(
            ctx,
            "Time Series:",
            &spec.borrow().timeseries,
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().timeseries = b;
                on_change()
            }),
        );
        common.add((l, e));
        let (l, e, keep_points) = source(
            ctx,
            "Keep Points:",
            &spec.borrow().keep_points,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().keep_points = s;
                on_change()
            }),
        );
        common.add((l, e));
        let addbtn = gtk::Button::with_label("+");
        root.pack_start(&addbtn, false, false, 0);
        let series = Rc::new(RefCell::new(Vec::new()));
        let seriesbox = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.pack_start(&seriesbox, true, true, 0);
        let build_series = Rc::new(clone!(
            @weak seriesbox,
            @strong ctx,
            @strong on_change,
            @strong spec,
            @strong series => move |i: usize| {
                let mut grid = TwoColGrid::new();
                seriesbox.pack_start(grid.root(), false, false, 0);
                let sep = gtk::Separator::new(gtk::Orientation::Vertical);
                grid.attach(&sep, 0, 2, 1);
                let (l, e, title) = source(
                    &ctx,
                    "Title:",
                    &spec.borrow().series[i].title,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].title = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let (l, e, x) = source(
                    &ctx,
                    "X:",
                    &spec.borrow().series[i].x,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].x = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let (l, e, y) = source(
                    &ctx,
                    "Y:",
                    &spec.borrow().series[i].y,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].y = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let remove = gtk::Button::with_label("-");
                grid.attach(&remove, 0, 2, 1);
                series.borrow_mut().push(Series { title, x, y });
                seriesbox.show_all();
                remove.connect_clicked(clone!(
                    @strong series,
                    @weak seriesbox,
                    @strong spec,
                    @strong on_change => move |_| {
                        series.borrow_mut().remove(i);
                        spec.borrow_mut().series.remove(i);
                        seriesbox.remove(&seriesbox.get_children()[i]);
                        on_change()
                    }));
        }));
        addbtn.connect_clicked(clone!(
            @strong spec, @strong build_series => move |_| {
                spec.borrow_mut().series.push(view::Series {
                    title: view::Source::Constant(Value::String(Chars::from("Series"))),
                    x: view::Source::Load(Path::from("/somewhere/in/netidx/x")),
                    y: view::Source::Load(Path::from("/somewhere/in/netidx/y")),
                });
                build_series(spec.borrow().series.len() - 1)
            }
        ));
        for i in 0..spec.borrow().series.len() {
            build_series(i)
        }
        LinePlot { root, spec, title, x_label, y_label, timeseries, keep_points, series }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::LinePlot(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, s)) = &*self.title.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.x_label.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.y_label.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.timeseries.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.keep_points.borrow() {
            s.update(tgt, value);
        }
        for s in self.series.borrow().iter() {
            if let Some((_, s)) = &*s.title.borrow() {
                s.update(tgt, value);
            }
            if let Some((_, s)) = &*s.x.borrow() {
                s.update(tgt, value);
            }
            if let Some((_, s)) = &*s.y.borrow() {
                s.update(tgt, value);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BoxChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::BoxChild>>,
}

impl BoxChild {
    fn new(on_change: OnChange, spec: view::BoxChild) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let packlbl = gtk::Label::new(Some("Pack:"));
        let packcb = gtk::ComboBoxText::new();
        packcb.append(Some("Start"), "Start");
        packcb.append(Some("End"), "End");
        packcb.set_active_id(Some(match spec.borrow().pack {
            view::Pack::Start => "Start",
            view::Pack::End => "End",
        }));
        packcb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().pack = match c.get_active_id() {
                Some(s) if &*s == "Start" => view::Pack::Start,
                Some(s) if &*s == "End" => view::Pack::End,
                _ => view::Pack::Start
            };
            on_change()
        }));
        root.add((packlbl, packcb));
        root.add(parse_entry(
            "Padding:",
            &spec.borrow().padding,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().padding = s;
                on_change()
            }),
        ));
        BoxChild { root, spec }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::BoxChild(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

fn dirselect(
    cur: view::Direction,
    on_change: impl Fn(view::Direction) + 'static,
) -> gtk::ComboBoxText {
    let dircb = gtk::ComboBoxText::new();
    dircb.append(Some("Horizontal"), "Horizontal");
    dircb.append(Some("Vertical"), "Vertical");
    match cur {
        view::Direction::Horizontal => dircb.set_active_id(Some("Horizontal")),
        view::Direction::Vertical => dircb.set_active_id(Some("Vertical")),
    };
    dircb.connect_changed(move |c| {
        on_change(match c.get_active_id() {
            Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
            Some(s) if &*s == "Vertical" => view::Direction::Vertical,
            _ => view::Direction::Horizontal,
        })
    });
    dircb
}

#[derive(Clone, Debug)]
struct BoxContainer {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Box>>,
}

impl BoxContainer {
    fn new(on_change: OnChange, spec: view::Box) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dircb = dirselect(
            spec.borrow().direction,
            clone!(@strong on_change, @strong spec => move |d| {
                spec.borrow_mut().direction = d;
                on_change()
            }),
        );
        let dirlbl = gtk::Label::new(Some("Direction:"));
        root.add((dirlbl, dircb));
        let homo = gtk::CheckButton::with_label("Homogeneous:");
        root.attach(&homo, 0, 2, 1);
        homo.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().homogeneous = b.get_active();
            on_change()
        }));
        root.add(parse_entry(
            "Spacing:",
            &spec.borrow().spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().spacing = s;
                on_change()
            }),
        ));
        BoxContainer { root, spec }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Box(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct GridChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::GridChild>>,
}

impl GridChild {
    fn new(on_change: OnChange, spec: view::GridChild) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Width:",
            &spec.borrow().width,
            clone!(@strong on_change, @strong spec => move |w| {
                spec.borrow_mut().width = w;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Height:",
            &spec.borrow().height,
            clone!(@strong on_change, @strong spec => move |h| {
                spec.borrow_mut().height = h;
                on_change()
            }),
        ));
        GridChild { root, spec }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::GridChild(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Grid {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Grid>>,
}

impl Grid {
    fn new(on_change: OnChange, spec: view::Grid) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Homogeneous Columns:",
            &spec.borrow().homogeneous_columns,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().homogeneous_columns = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Homogeneous Rows:",
            &spec.borrow().homogeneous_rows,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().homogeneous_rows = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Column Spacing:",
            &spec.borrow().column_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().column_spacing = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Row Spacing:",
            &spec.borrow().row_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().row_spacing = s;
                on_change()
            }),
        ));
        Grid { root, spec }
    }

    fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Grid(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Debug, Clone)]
struct WidgetProps {
    root: gtk::Expander,
    spec: Rc<RefCell<view::WidgetProps>>,
}

impl WidgetProps {
    fn new(on_change: OnChange, spec: view::WidgetProps) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Expander::new(Some("Layout Properties"));
        let mut grid = TwoColGrid::new();
        root.add(grid.root());
        let aligns = ["Fill", "Start", "End", "Center", "Baseline"];
        fn align_to_str(a: view::Align) -> &'static str {
            match a {
                view::Align::Fill => "Fill",
                view::Align::Start => "Start",
                view::Align::End => "End",
                view::Align::Center => "Center",
                view::Align::Baseline => "Baseline",
            }
        }
        fn align_from_str(a: GString) -> view::Align {
            match &*a {
                "Fill" => view::Align::Fill,
                "Start" => view::Align::Start,
                "End" => view::Align::End,
                "Center" => view::Align::Center,
                "Baseline" => view::Align::Baseline,
                x => unreachable!(x),
            }
        }
        let halign_lbl = gtk::Label::new(Some("Horizontal Alignment:"));
        let halign = gtk::ComboBoxText::new();
        let valign_lbl = gtk::Label::new(Some("Vertical Alignment:"));
        let valign = gtk::ComboBoxText::new();
        grid.add((halign_lbl.clone(), halign.clone()));
        grid.add((valign_lbl.clone(), valign.clone()));
        for a in &aligns {
            halign.append(Some(a), a);
            valign.append(Some(a), a);
        }
        halign.set_active_id(Some(align_to_str(spec.borrow().halign)));
        valign.set_active_id(Some(align_to_str(spec.borrow().valign)));
        halign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().halign =
                c.get_active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            on_change()
        }));
        valign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().valign =
                c.get_active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            on_change()
        }));
        let hexp = gtk::CheckButton::with_label("Expand Horizontally");
        grid.attach(&hexp, 0, 2, 1);
        hexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            spec.borrow_mut().hexpand = b.get_active();
            on_change()
        }));
        let vexp = gtk::CheckButton::with_label("Expand Vertically");
        grid.attach(&vexp, 0, 2, 1);
        vexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            spec.borrow_mut().vexpand = b.get_active();
            on_change()
        }));
        grid.add(parse_entry(
            "Top Margin:",
            &spec.borrow().margin_top,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin_top = s;
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Bottom Margin:",
            &spec.borrow().margin_bottom,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin_bottom = s;
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Start Margin:",
            &spec.borrow().margin_start,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin_start = s;
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "End Margin:",
            &spec.borrow().margin_end,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin_end = s;
                on_change()
            }),
        ));
        WidgetProps { root, spec }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn spec(&self) -> view::WidgetProps {
        self.spec.borrow().clone()
    }
}

#[derive(Clone, Debug)]
enum WidgetKind {
    Action(Action),
    Table(Table),
    Label(Label),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    LinePlot(LinePlot),
    Box(BoxContainer),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
    GridRow,
}

impl WidgetKind {
    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            WidgetKind::Action(w) => Some(w.root()),
            WidgetKind::Table(w) => Some(w.root()),
            WidgetKind::Label(w) => Some(w.root()),
            WidgetKind::Button(w) => Some(w.root()),
            WidgetKind::Toggle(w) => Some(w.root()),
            WidgetKind::Selector(w) => Some(w.root()),
            WidgetKind::Entry(w) => Some(w.root()),
            WidgetKind::LinePlot(w) => Some(w.root()),
            WidgetKind::Box(w) => Some(w.root()),
            WidgetKind::BoxChild(w) => Some(w.root()),
            WidgetKind::Grid(w) => Some(w.root()),
            WidgetKind::GridChild(w) => Some(w.root()),
            WidgetKind::GridRow => None,
        }
    }
}

#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "NetidxEditorWidget")]
struct Widget {
    root: gtk::Box,
    props: Option<WidgetProps>,
    kind: WidgetKind,
}

impl Widget {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Widget,
    ) {
        let (name, kind, props) = match spec {
            view::Widget { props: _, kind: view::WidgetKind::Action(s) } => (
                "Action",
                WidgetKind::Action(Action::new(ctx, on_change.clone(), s)),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::Table(s) } => (
                "Table",
                WidgetKind::Table(Table::new(on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Label(s) } => (
                "Label",
                WidgetKind::Label(Label::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Button(s) } => (
                "Button",
                WidgetKind::Button(Button::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Toggle(s) } => (
                "Toggle",
                WidgetKind::Toggle(Toggle::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Selector(s) } => (
                "Selector",
                WidgetKind::Selector(Selector::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Entry(s) } => (
                "Entry",
                WidgetKind::Entry(Entry::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::LinePlot(s) } => (
                "LinePlot",
                WidgetKind::LinePlot(LinePlot::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Box(s) } => (
                "Box",
                WidgetKind::Box(BoxContainer::new(on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::BoxChild(s) } => {
                ("BoxChild", WidgetKind::BoxChild(BoxChild::new(on_change, s)), None)
            }
            view::Widget { props, kind: view::WidgetKind::Grid(s) } => (
                "Grid",
                WidgetKind::Grid(Grid::new(on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::GridChild(s) } => {
                ("GridChild", WidgetKind::GridChild(GridChild::new(on_change, s)), None)
            }
            view::Widget { props: _, kind: view::WidgetKind::GridRow(_) } => {
                ("GridRow", WidgetKind::GridRow, None)
            }
        };
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        if let Some(p) = props.as_ref() {
            root.pack_start(p.root(), false, false, 0);
        }
        if let Some(r) = kind.root() {
            let exp = gtk::Expander::new(Some("Widget Config"));
            exp.add(r);
            exp.set_expanded(true);
            root.pack_start(&exp, false, false, 0);
        }
        store.set_value(iter, 0, &name.to_value());
        let t = Widget { root, props, kind };
        store.set_value(iter, 1, &t.to_value());
    }

    fn spec(&self) -> view::Widget {
        let props = self.props.as_ref().map(|p| p.spec()).unwrap_or(DEFAULT_PROPS);
        let kind = match &self.kind {
            WidgetKind::Action(w) => w.spec(),
            WidgetKind::Table(w) => w.spec(),
            WidgetKind::Label(w) => w.spec(),
            WidgetKind::Button(w) => w.spec(),
            WidgetKind::Toggle(w) => w.spec(),
            WidgetKind::Selector(w) => w.spec(),
            WidgetKind::Entry(w) => w.spec(),
            WidgetKind::LinePlot(w) => w.spec(),
            WidgetKind::Box(w) => w.spec(),
            WidgetKind::BoxChild(w) => w.spec(),
            WidgetKind::Grid(w) => w.spec(),
            WidgetKind::GridChild(w) => w.spec(),
            WidgetKind::GridRow => {
                view::WidgetKind::GridRow(view::GridRow { columns: vec![] })
            }
        };
        view::Widget { props, kind }
    }

    fn default_spec(name: Option<&str>) -> view::Widget {
        fn widget(kind: view::WidgetKind) -> view::Widget {
            view::Widget { kind, props: DEFAULT_PROPS }
        }
        match name {
            None => widget(view::WidgetKind::Table(Path::from("/"))),
            Some("Action") => widget(view::WidgetKind::Action(view::Action {
                source: view::Source::Constant(Value::U64(42)),
                sink: view::Sink::Variable(String::from("foo")),
            })),
            Some("Table") => widget(view::WidgetKind::Table(Path::from("/"))),
            Some("Label") => {
                let s = Value::String(Chars::from("static label"));
                widget(view::WidgetKind::Label(view::Source::Constant(s)))
            }
            Some("Button") => {
                let l = Chars::from("click me!");
                widget(view::WidgetKind::Button(view::Button {
                    enabled: view::Source::Constant(Value::True),
                    label: view::Source::Constant(Value::String(l)),
                    source: view::Source::Load(Path::from("/somewhere")),
                    sink: view::Sink::Store(Path::from("/somewhere/else")),
                }))
            }
            Some("Toggle") => widget(view::WidgetKind::Toggle(view::Toggle {
                enabled: view::Source::Constant(Value::True),
                source: view::Source::Load(Path::from("/somewhere")),
                sink: view::Sink::Store(Path::from("/somewhere/else")),
            })),
            Some("Selector") => {
                let choices =
                    Chars::from(r#"[[{"U64": 1}, "One"], [{"U64": 2}, "Two"]]"#);
                widget(view::WidgetKind::Selector(view::Selector {
                    enabled: view::Source::Constant(Value::True),
                    choices: view::Source::Constant(Value::String(choices)),
                    source: view::Source::Load(Path::from("/somewhere")),
                    sink: view::Sink::Store(Path::from("/somewhere/else")),
                }))
            }
            Some("Entry") => widget(view::WidgetKind::Entry(view::Entry {
                enabled: view::Source::Constant(Value::True),
                visible: view::Source::Constant(Value::True),
                source: view::Source::Load(Path::from("/somewhere")),
                sink: view::Sink::Store(Path::from("/somewhere/else")),
            })),
            Some("LinePlot") => widget(view::WidgetKind::LinePlot(view::LinePlot {
                title: view::Source::Constant(Value::String(Chars::from("Line Plot"))),
                x_label: view::Source::Constant(Value::String(Chars::from("x axis"))),
                y_label: view::Source::Constant(Value::String(Chars::from("y axis"))),
                timeseries: view::Source::Constant(Value::False),
                keep_points: view::Source::Constant(Value::U64(256)),
                series: Vec::new(),
            })),
            Some("Box") => widget(view::WidgetKind::Box(view::Box {
                direction: view::Direction::Vertical,
                homogeneous: false,
                spacing: 0,
                children: Vec::new(),
            })),
            Some("BoxChild") => {
                let s = Value::String(Chars::from("empty box child"));
                let w = view::Widget {
                    kind: view::WidgetKind::Label(view::Source::Constant(s)),
                    props: DEFAULT_PROPS,
                };
                widget(view::WidgetKind::BoxChild(view::BoxChild {
                    pack: view::Pack::Start,
                    padding: 0,
                    widget: boxed::Box::new(w),
                }))
            }
            Some("Grid") => widget(view::WidgetKind::Grid(view::Grid {
                homogeneous_columns: false,
                homogeneous_rows: false,
                column_spacing: 0,
                row_spacing: 0,
                rows: Vec::new(),
            })),
            Some("GridChild") => {
                let s = Value::String(Chars::from("empty grid child"));
                let w = view::Widget {
                    kind: view::WidgetKind::Label(view::Source::Constant(s)),
                    props: DEFAULT_PROPS,
                };
                widget(view::WidgetKind::GridChild(view::GridChild {
                    width: 1,
                    height: 1,
                    widget: boxed::Box::new(w),
                }))
            }
            Some("GridRow") => {
                widget(view::WidgetKind::GridRow(view::GridRow { columns: vec![] }))
            }
            _ => unreachable!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(&self, tgt: Target, value: &Value) {
        match &self.kind {
            WidgetKind::Action(w) => w.update(tgt, value),
            WidgetKind::Table(_) => (),
            WidgetKind::Label(w) => w.update(tgt, value),
            WidgetKind::Button(w) => w.update(tgt, value),
            WidgetKind::Toggle(w) => w.update(tgt, value),
            WidgetKind::Selector(w) => w.update(tgt, value),
            WidgetKind::Entry(w) => w.update(tgt, value),
            WidgetKind::LinePlot(w) => w.update(tgt, value),
            WidgetKind::Box(_)
            | WidgetKind::BoxChild(_)
            | WidgetKind::Grid(_)
            | WidgetKind::GridChild(_)
            | WidgetKind::GridRow => (),
        }
    }
}

pub(super) struct Editor {
    root: gtk::Box,
    store: gtk::TreeStore,
}

static KINDS: [&'static str; 13] = [
    "Action",
    "Table",
    "Label",
    "Button",
    "Toggle",
    "Selector",
    "Entry",
    "LinePlot",
    "Box",
    "BoxChild",
    "Grid",
    "GridChild",
    "GridRow",
];

impl Editor {
    pub(super) fn new(ctx: WidgetCtx, spec: view::View) -> Editor {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.set_margin_start(5);
        root.set_margin_end(5);
        let treebtns = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        root.pack_start(&treebtns, false, false, 0);
        let addbtnicon = gtk::Image::from_icon_name(
            Some("list-add-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let addbtn = gtk::ToolButton::new(Some(&addbtnicon), None);
        let addchbtnicon = gtk::Image::from_icon_name(
            Some("go-down-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let addchbtn = gtk::ToolButton::new(Some(&addchbtnicon), None);
        let delbtnicon = gtk::Image::from_icon_name(
            Some("list-remove-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let delbtn = gtk::ToolButton::new(Some(&delbtnicon), None);
        let dupbtnicon = gtk::Image::from_icon_name(
            Some("edit-copy-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let dupbtn = gtk::ToolButton::new(Some(&dupbtnicon), None);
        treebtns.pack_start(&addbtn, false, false, 5);
        treebtns.pack_start(&addchbtn, false, false, 5);
        treebtns.pack_start(&delbtn, false, false, 5);
        treebtns.pack_start(&dupbtn, false, false, 5);
        let treewin =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        treewin.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        root.pack_start(&treewin, true, true, 5);
        let view = gtk::TreeView::new();
        treewin.add(&view);
        view.append_column(&{
            let column = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("widget tree");
            column.add_attribute(&cell, "text", 0);
            column
        });
        let store = gtk::TreeStore::new(&[String::static_type(), Widget::static_type()]);
        view.set_model(Some(&store));
        view.set_reorderable(true);
        view.set_enable_tree_lines(true);
        let spec = Rc::new(RefCell::new(spec));
        let on_change: OnChange = Rc::new({
            let ctx = ctx.clone();
            let spec = Rc::clone(&spec);
            let store = store.clone();
            let scheduled = Rc::new(Cell::new(false));
            move || {
                if !scheduled.get() {
                    scheduled.set(true);
                    idle_add_local(clone!(
                        @strong ctx,
                        @strong spec,
                        @strong store,
                        @strong scheduled => move || {
                        if let Some(root) = store.get_iter_first() {
                            spec.borrow_mut().root = Editor::build_spec(&store, &root);
                            let m = FromGui::Render(spec.borrow().clone());
                            let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                        }
                        scheduled.set(false);
                        glib::Continue(false)
                    }));
                }
            }
        });
        Editor::build_tree(&ctx, &on_change, &store, None, &spec.borrow().root);
        let selected: Rc<RefCell<Option<gtk::TreeIter>>> = Rc::new(RefCell::new(None));
        let reveal_properties = gtk::Revealer::new();
        root.pack_end(&reveal_properties, false, false, 5);
        let properties = gtk::Box::new(gtk::Orientation::Vertical, 5);
        reveal_properties.add(&properties);
        let inhibit_change = Rc::new(Cell::new(false));
        let kind = gtk::ComboBoxText::new();
        for k in &KINDS {
            kind.append(Some(k), k);
        }
        kind.connect_changed(clone!(
            @strong on_change,
            @strong store,
            @strong selected,
            @strong ctx,
            @weak properties,
            @strong inhibit_change => move |c| {
            if let Some(iter) = selected.borrow().clone() {
                if !inhibit_change.get() {
                    let wv = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = wv.get::<&Widget>() {
                        properties.remove(w.root());
                    }
                    let id = c.get_active_id();
                    let spec = Widget::default_spec(id.as_ref().map(|s| &**s));
                    Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
                    let wv = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = wv.get::<&Widget>() {
                        properties.pack_start(w.root(), true, true, 5);
                    }
                    on_change();
                }
            }
        }));
        properties.pack_start(&kind, false, false, 0);
        properties.pack_start(
            &gtk::Separator::new(gtk::Orientation::Vertical),
            false,
            false,
            0,
        );
        let selection = view.get_selection();
        selection.set_mode(gtk::SelectionMode::Single);
        selection.connect_changed(clone!(
        @strong ctx,
        @strong selected,
        @weak store,
        @weak kind,
        @weak reveal_properties,
        @weak properties,
        @strong inhibit_change => move |s| {
            let children = properties.get_children();
            if children.len() == 3 {
                properties.remove(&children[2]);
            }
            match s.get_selected() {
                None => {
                    *selected.borrow_mut() = None;
                    let _: result::Result<_, _> =
                        ctx.to_gui.send(ToGui::Highlight(vec![]));
                    reveal_properties.set_reveal_child(false);
                }
                Some((_, iter)) => {
                    *selected.borrow_mut() = Some(iter.clone());
                    let mut path = Vec::new();
                    Editor::build_widget_path(&store, &iter, 0, 0, &mut path);
                    let _: result::Result<_,_> = ctx.to_gui.send(ToGui::Highlight(path));
                    let v = store.get_value(&iter, 0);
                    if let Ok(Some(id)) = v.get::<&str>() {
                        inhibit_change.set(true);
                        kind.set_active_id(Some(id));
                        inhibit_change.set(false);
                    }
                    let v = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = v.get::<&Widget>() {
                        properties.pack_start(w.root(), true, true, 5);
                    }
                    properties.show_all();
                    reveal_properties.set_reveal_child(true);
                }
            }
        }));
        let menu = gtk::Menu::new();
        let duplicate = gtk::MenuItem::with_label("Duplicate");
        let new_sib = gtk::MenuItem::with_label("New Sibling");
        let new_child = gtk::MenuItem::with_label("New Child");
        let delete = gtk::MenuItem::with_label("Delete");
        menu.append(&duplicate);
        menu.append(&new_sib);
        menu.append(&new_child);
        menu.append(&delete);
        let dup = Rc::new(clone!(
        @strong on_change, @weak store, @strong selected, @strong ctx => move || {
            if let Some(iter) = &*selected.borrow() {
                let spec = Editor::build_spec(&store, iter);
                let parent = store.iter_parent(iter);
                Editor::build_tree(&ctx, &on_change, &store, parent.as_ref(), &spec);
                on_change()
            }
        }));
        duplicate.connect_activate(clone!(@strong dup => move |_| dup()));
        dupbtn.connect_clicked(clone!(@strong dup => move |_| dup()));
        let newsib = Rc::new(clone!(
            @strong on_change, @weak store, @strong selected, @strong ctx => move || {
            let iter = store.insert_after(None, selected.borrow().as_ref());
            let spec = Widget::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        new_sib.connect_activate(clone!(@strong newsib => move |_| newsib()));
        addbtn.connect_clicked(clone!(@strong newsib => move |_| newsib()));
        let newch = Rc::new(clone!(
            @strong on_change, @weak store, @strong selected, @strong ctx => move || {
            let iter = store.insert_after(selected.borrow().as_ref(), None);
            let spec = Widget::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        new_child.connect_activate(clone!(@strong newch => move |_| newch()));
        addchbtn.connect_clicked(clone!(@strong newch => move |_| newch()));
        let del = Rc::new(clone!(
            @weak selection, @strong on_change, @weak store, @strong selected => move || {
            let iter = selected.borrow().clone();
            if let Some(iter) = iter {
                selection.unselect_iter(&iter);
                store.remove(&iter);
                on_change();
            }
        }));
        delete.connect_activate(clone!(@strong del => move |_| del()));
        delbtn.connect_clicked(clone!(@strong del => move |_| del()));
        view.connect_button_press_event(move |_, b| {
            let right_click =
                gdk::EventType::ButtonPress == b.get_event_type() && b.get_button() == 3;
            if right_click {
                menu.show_all();
                menu.popup_at_pointer(Some(&*b));
                Inhibit(true)
            } else {
                Inhibit(false)
            }
        });
        store.connect_row_deleted(clone!(@strong on_change => move |_, _| {
            on_change();
        }));
        store.connect_row_inserted(clone!(@strong on_change => move |_, _, _| {
            on_change();
        }));
        Editor { root, store }
    }

    fn build_tree(
        ctx: &WidgetCtx,
        on_change: &OnChange,
        store: &gtk::TreeStore,
        parent: Option<&gtk::TreeIter>,
        w: &view::Widget,
    ) {
        let iter = store.insert_before(parent, None);
        Widget::insert(ctx, on_change.clone(), store, &iter, w.clone());
        match &w.kind {
            view::WidgetKind::Box(b) => {
                for w in &b.children {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::BoxChild(b) => {
                Editor::build_tree(ctx, on_change, store, Some(&iter), &*b.widget)
            }
            view::WidgetKind::Grid(g) => {
                for w in &g.rows {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::GridChild(g) => {
                Editor::build_tree(ctx, on_change, store, Some(&iter), &*g.widget)
            }
            view::WidgetKind::GridRow(g) => {
                for w in &g.columns {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::Action(_)
            | view::WidgetKind::Table(_)
            | view::WidgetKind::Label(_)
            | view::WidgetKind::Button(_)
            | view::WidgetKind::Toggle(_)
            | view::WidgetKind::Selector(_)
            | view::WidgetKind::Entry(_)
            | view::WidgetKind::LinePlot(_) => (),
        }
    }

    fn build_spec(store: &gtk::TreeStore, root: &gtk::TreeIter) -> view::Widget {
        let v = store.get_value(root, 1);
        match v.get::<&Widget>() {
            Err(e) => {
                let s = Value::String(Chars::from(format!("tree error: {}", e)));
                view::Widget {
                    kind: view::WidgetKind::Label(view::Source::Constant(s)),
                    props: DEFAULT_PROPS,
                }
            }
            Ok(None) => {
                let s = Value::String(Chars::from("tree error: missing widget"));
                view::Widget {
                    kind: view::WidgetKind::Label(view::Source::Constant(s)),
                    props: DEFAULT_PROPS,
                }
            }
            Ok(Some(w)) => {
                let mut spec = w.spec();
                match &mut spec.kind {
                    view::WidgetKind::Box(ref mut b) => {
                        b.children.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                b.children.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    view::WidgetKind::BoxChild(ref mut b) => {
                        if let Some(iter) = store.iter_children(Some(root)) {
                            b.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        }
                    }
                    view::WidgetKind::GridChild(ref mut g) => {
                        if let Some(iter) = store.iter_children(Some(root)) {
                            g.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        }
                    }
                    view::WidgetKind::Grid(ref mut g) => {
                        g.rows.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                g.rows.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    view::WidgetKind::GridRow(ref mut g) => {
                        g.columns.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                g.columns.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    _ => (),
                };
                spec
            }
        }
    }

    fn build_widget_path(
        store: &gtk::TreeStore,
        start: &gtk::TreeIter,
        mut nrow: usize,
        nchild: usize,
        path: &mut Vec<WidgetPath>,
    ) {
        let v = store.get_value(start, 1);
        let skip_idx = match v.get::<&Widget>() {
            Err(_) | Ok(None) => false,
            Ok(Some(w)) => match &w.kind {
                WidgetKind::Action(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Table(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Label(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Button(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Toggle(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Selector(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Entry(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::LinePlot(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Box(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        path.insert(0, WidgetPath::Box(nchild));
                    }
                    false
                }
                WidgetKind::BoxChild(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                WidgetKind::Grid(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf)
                    } else if path.len() == 1 {
                        match path[0] {
                            WidgetPath::GridRow(_) => (),
                            _ => path.insert(0, WidgetPath::GridItem(nrow, nchild)),
                        }
                    } else {
                        path.insert(0, WidgetPath::GridItem(nrow, nchild))
                    }
                    false
                }
                WidgetKind::GridChild(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                WidgetKind::GridRow => {
                    if let Some(idx) = store.get_path(start).map(|t| t.get_indices()) {
                        if let Some(i) = idx.last() {
                            nrow = *i as usize;
                        }
                    }
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::GridRow(nrow));
                    }
                    true
                }
            },
        };
        if let Some(parent) = store.iter_parent(start) {
            if let Some(idx) = store.get_path(start).map(|t| t.get_indices()) {
                if let Some(i) = idx.last() {
                    let nchild = if skip_idx { nchild } else { *i as usize };
                    Editor::build_widget_path(store, &parent, nrow, nchild, path);
                }
            }
        };
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        self.store.foreach(|store, _, iter| {
            let v = store.get_value(iter, 1);
            match v.get::<&Widget>() {
                Err(_) | Ok(None) => false,
                Ok(Some(w)) => {
                    w.update(tgt, value);
                    false
                }
            }
        })
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}