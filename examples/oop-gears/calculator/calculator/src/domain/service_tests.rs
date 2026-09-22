// Created: 2026-04-07 by Constructor Tech
use super::*;

#[test]
fn test_add() {
    let service = Service::new();
    assert_eq!(service.add(10, 20), 30);
}

#[test]
fn test_add_negative() {
    let service = Service::new();
    assert_eq!(service.add(-5, 3), -2);
}

#[test]
fn test_sub() {
    let service = Service::new();
    assert_eq!(service.sub(20, 8), 12);
}
