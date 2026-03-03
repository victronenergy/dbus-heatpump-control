# Venus OS Heat Pump Control Service

`com.victronenergy.heatpumpcontrol`

A Venus OS service that enables **SG-Ready control** for heat pumps, boilers and similar appliances using:

- GX device internal relay 2
- A Victron VM-3P75CT energy meter (for monitoring heatpump power consumption)
- Integration into [venus-opportunity-loads](https://github.com/victronenergy/venus-opportunity-loads) via S2 over D-Bus

This allows the heat pump to be integrated into energy management via its [SG-Ready](https://www.waermepumpe.de/fileadmin/user_upload/bwp_service/SG_ready/SG_Ready_Interface_1.1.pdf) interface.

## SG-Ready and Operating State 3

**SG-Ready (Smart Grid Ready)** is a standardized control interface used by many modern heat pumps to allow external management systems to influence their operation. It uses two digital inputs to select between defined operating modes. These modes enable grid- or energy-aware behavior, such as reducing consumption, running normally, or increasing consumption when surplus energy is available.

**Operating State 3** (often referred to as "Boost" or "Recommended ON") signals the heat pump to actively run and make use of available surplus energy — for example, from photovoltaic production. In this mode, the heat pump may increase compressor activity or prioritize heating/storage in order to absorb excess energy. Within this service, the GX device’s internal Relay 2 is used to activate the SG-Ready input corresponding to Operating Mode 3, allowing the heat pump to participate in smart energy management while still maintaining its internal safety and temperature controls.

While the interface offers only limited control capabilities, its widespread integration across many heat pumps and its inherent simplicity position it well for broad market adoption.

## Requirements

- Victron Energy VM-3P75CT
  - Configured to the `heatpump` role
  - Only a single Heat pump energy meter in a system is allowed
  - Correct configuration of number and phase assignment is required

- GX Device
  - GX Relay 2 NO/COM terminals wired to SG-Ready input for "Boost" state of the heat pump
  - GX Relay 2 configured to function `6`
  - "Opportunity Loads" installed and enabled

## Working principle

The service uses the power measurements provided by the VM-3P75CT to learn the heatpump’s power consumption during SG-Ready Operating State 3.

It detects "running" states based on a threshold, stores recent running samples in a rolling time window, and derives a representative expected power using a configurable quantile combined with exponential smoothing (EWMA).

This enables the system to model the heatpump as a stable and predictable load for S2's Operation Mode Based Control (OMBC), even though SG-Ready itself only offers binary ON/OFF signaling.
