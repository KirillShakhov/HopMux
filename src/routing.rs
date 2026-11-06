use crate::config::{Config, Route};
use std::collections::{BTreeMap, BTreeSet};

const MAX_COMPOSED_ROUTES: usize = 4096;

type Adjacency = BTreeMap<String, Vec<(usize, String)>>;
type Candidates = BTreeMap<String, BTreeMap<Vec<String>, (Vec<usize>, usize)>>;

pub fn discover(config: &Config) -> (Vec<Route>, bool) {
    let mut edges: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for (priority, path) in config.paths.iter().enumerate() {
        for pair in path.windows(2) {
            let next = edges.entry(pair[0].clone()).or_default();
            let rank = next.entry(pair[1].clone()).or_insert(priority);
            *rank = (*rank).min(priority);
        }
    }
    let adjacency: Adjacency = edges.into_iter().map(|(from, edges)| {
        let mut edges: Vec<_> = edges.into_iter().map(|(to, priority)| (priority, to)).collect();
        edges.sort();
        (from, edges)
    }).collect();

    let mut candidates = Candidates::new();
    let mut count = 0;
    let mut overflow = false;
    for (direct_priority, host) in config.direct_hosts.iter().enumerate() {
        let mut hops = vec![host.clone()];
        let mut priorities = Vec::new();
        let mut visited = BTreeSet::from([host.clone()]);
        walk(host, &adjacency, &mut hops, &mut priorities, &mut visited,
            direct_priority, &mut candidates, &mut count, &mut overflow);
        if overflow { break; }
    }

    let mut routes = Vec::new();
    for (target, candidates) in candidates {
        let mut candidates: Vec<_> = candidates.into_iter().collect();
        candidates.sort_by(|(left_hops, (left_priority, left_direct)),
                            (right_hops, (right_priority, right_direct))| {
            left_priority.cmp(right_priority)
                .then(left_direct.cmp(right_direct))
                .then(left_hops.cmp(right_hops))
        });
        for (index, (hops, _)) in candidates.into_iter().enumerate() {
            routes.push(Route {
                name: format!("{target}-{}", index + 1),
                target: target.clone(),
                hops,
                priority: index as i32,
                disabled: false,
                addresses: BTreeMap::new(),
            });
        }
    }
    (routes, overflow)
}

fn walk(
    current: &str,
    adjacency: &Adjacency,
    hops: &mut Vec<String>,
    priorities: &mut Vec<usize>,
    visited: &mut BTreeSet<String>,
    direct_priority: usize,
    candidates: &mut Candidates,
    count: &mut usize,
    overflow: &mut bool,
) {
    if *count >= MAX_COMPOSED_ROUTES { *overflow = true; return; }
    let by_hops = candidates.entry(current.to_owned()).or_default();
    let candidate = (priorities.clone(), direct_priority);
    match by_hops.get_mut(hops) {
        Some(previous) if candidate < *previous => *previous = candidate,
        Some(_) => {},
        None => { by_hops.insert(hops.clone(), candidate); *count += 1; }
    }
    if let Some(edges) = adjacency.get(current) {
        for (priority, next) in edges {
            if !visited.insert(next.clone()) { continue; }
            hops.push(next.clone());
            priorities.push(*priority);
            walk(next, adjacency, hops, priorities, visited, direct_priority,
                candidates, count, overflow);
            priorities.pop();
            hops.pop();
            visited.remove(next);
            if *overflow { return; }
        }
    }
}
