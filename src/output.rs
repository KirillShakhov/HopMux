use crate::{config::{Config, Route}, transfer::{Direction, Journal}};

pub fn route_chain(hops: &[String]) -> String {
    std::iter::once("local")
        .chain(hops.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" -> ")
}

pub fn route_label(route: &Route) -> String {
    format!("{}: {}", route.name, route_chain(&route.hops))
}

pub fn route_label_by_name(config: &Config, target: &str, name: &str) -> String {
    config.effective_routes().into_iter()
        .find(|route| route.target == target && route.name == name)
        .map(|route| route_label(&route))
        .unwrap_or_else(|| format!("{name}: local -> [unknown hops] -> {target}"))
}

pub fn optional_ms(value: Option<u128>) -> String {
    value.map_or_else(|| "-".to_owned(), |milliseconds| format!("{milliseconds}ms"))
}

pub fn transfer_chain(journal: &Journal) -> String {
    match journal.direction {
        Direction::Put => format!("local:{} -> {}:{}", journal.source, journal.target, journal.destination),
        Direction::Get => format!("{}:{} -> local:{}", journal.target, journal.source, journal.destination),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_show_every_hop_in_order() {
        assert_eq!(route_chain(&["entry".into(), "relay".into(), "target".into()]),
            "local -> entry -> relay -> target");
        assert_eq!(route_chain(&["target".into()]), "local -> target");
    }

    #[test]
    fn durations_do_not_show_debug_option_syntax() {
        assert_eq!(optional_ms(Some(17)), "17ms");
        assert_eq!(optional_ms(None), "-");
    }
}
