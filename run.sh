name: 🐛 Bug Report
description: Report a bug or unexpected behavior in APEX Trading AI
labels: ["bug", "needs-triage"]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for reporting a bug! Please fill out as much detail as possible.

  - type: input
    id: version
    attributes:
      label: APEX Version
      description: Found in Settings → Add-ons → APEX Trading AI
      placeholder: "1.0.0"
    validations:
      required: true

  - type: input
    id: ha_version
    attributes:
      label: Home Assistant Version
      placeholder: "2024.1.0"
    validations:
      required: true

  - type: dropdown
    id: arch
    attributes:
      label: Architecture
      options:
        - amd64 (most PCs/Intel NUC)
        - aarch64 (Raspberry Pi 4, 64-bit)
        - armv7 (Raspberry Pi 3/4, 32-bit)
        - armhf (Raspberry Pi 2)
        - i386 (older 32-bit hardware)
    validations:
      required: true

  - type: dropdown
    id: install_type
    attributes:
      label: Installation Type
      options:
        - Home Assistant OS (HAOS)
        - Home Assistant Supervised
        - Home Assistant Container
        - Other
    validations:
      required: true

  - type: textarea
    id: description
    attributes:
      label: Bug Description
      description: What happened? What did you expect to happen?
      placeholder: "When I click 'Connect Broker', the page shows an error..."
    validations:
      required: true

  - type: textarea
    id: steps
    attributes:
      label: Steps to Reproduce
      placeholder: |
        1. Go to '...'
        2. Click on '...'
        3. See error
    validations:
      required: true

  - type: textarea
    id: logs
    attributes:
      label: Add-on Logs
      description: Paste relevant log output from Settings → Add-ons → APEX → Log tab
      render: shell

  - type: dropdown
    id: broker
    attributes:
      label: Broker (if relevant)
      options:
        - Not applicable
        - Alpaca
        - Binance
        - Coinbase
        - Kraken
        - Interactive Brokers
        - Other

  - type: dropdown
    id: market_data
    attributes:
      label: Market Data Source
      options:
        - Yahoo Finance
        - Finnhub
        - Alpha Vantage
        - Simulated

  - type: checkboxes
    id: checklist
    attributes:
      label: Before Submitting
      options:
        - label: I checked existing issues and this hasn't been reported
          required: true
        - label: I verified the add-on logs for error messages
          required: true
        - label: I am running the latest version of APEX
          required: true
