# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Thanos Makatos <thanos.makatos@nutanix.com>

TARGET ?= debug

.PHONY: all
all: $(TARGET)

.PHONY: debug
debug:
	$(CARGO_BUILD)

.PHONY: release
release:
	$(CARGO_BUILD) --release

CARGO_FMT_FLAGS = --config wrap_comments=true
CARGO_FMT = cargo fmt -- $(CARGO_FMT_FLAGS)
CARGO_CLIPPY = cargo clippy --tests --locked
CARGO_CLIPPY_FLAGS = --all-features --all-targets -- \
	-D warnings -D clippy::use_self -W dead_code
CARGO_BUILD = cargo build --locked

.PHONY: check
check:
	$(CARGO_FMT) --check
	$(CARGO_CLIPPY) $(CARGO_CLIPPY_FLAGS)

.PHONY: fix
fix:
	$(CARGO_FMT)
	$(CARGO_CLIPPY) --fix --allow-dirty --allow-staged \
		$(CARGO_CLIPPY_FLAGS)

.PHONY: clean
clean:
	cargo clean

.PHONY: unit-test
unit-test:
	RUST_BACKTRACE=1 cargo test

.PHONY: test
test: unit-test

.PHONY: pre-push
pre-push: check test

.PHONY: install
install: $(TARGET)
	install -D -m 0755 target/$(TARGET)/io-thread-controller \
		${DESTDIR}/usr/libexec/io-thread-controller
	install -D -m 0644 io-thread-controller.json \
		${DESTDIR}/etc/io-thread-controller.json
	install -D -m 0644 io-thread-controller.d/engines/threshold.json \
		${DESTDIR}/etc/io-thread-controller.d/engines/threshold.json
	install -D -m 0644 io-thread-controller.service \
		${DESTDIR}/usr/lib/systemd/system/io-thread-controller.service
	install -D -m 0644 com.nutanix.io_thread_controller.conf \
		${DESTDIR}/etc/dbus-1/system.d/com.nutanix.io_thread_controller.conf
