/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.examples.table_java.descriptors;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ExternalCatalogDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_FARES_FILE;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_MAX_EVENT_DELAY_SECS;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_RIDES_FILE;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_TYPE_VALUE_TAXI_DATA;

/**
 * A descriptor for the catalog of taxi data as provided by the New York City Taxi & Limousine Commission.
 */
public class TaxiData extends ExternalCatalogDescriptor {
	public TaxiData() {
		super(CATALOG_TYPE_VALUE_TAXI_DATA, 1);
	}

	private String ridesFile;
	private String faresFile;
	private Integer maxEventDelaySecs;
	private Integer servingSpeedFactor;

	public TaxiData ridesFile(String path) {
		this.ridesFile = Preconditions.checkNotNull(path);
		return this;
	}

	public TaxiData faresFile(String path) {
		this.faresFile = Preconditions.checkNotNull(path);
		return this;
	}

	public TaxiData maxEventDelaySecs(int maxEventDelaySecs) {
		this.maxEventDelaySecs = maxEventDelaySecs;
		return this;
	}

	public TaxiData servingSpeedFactor(int servingSpeedFactor) {
		this.servingSpeedFactor = servingSpeedFactor;
		return this;
	}

	@Override
	protected Map<String, String> toCatalogProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (this.ridesFile != null) {
			properties.putString(CATALOG_RIDES_FILE, this.ridesFile);
		}
		if (this.ridesFile != null) {
			properties.putString(CATALOG_FARES_FILE, this.faresFile);
		}
		if (this.maxEventDelaySecs != null) {
			properties.putInt(CATALOG_MAX_EVENT_DELAY_SECS, this.maxEventDelaySecs);
		}
		if (this.servingSpeedFactor != null) {
			properties.putInt(CATALOG_MAX_EVENT_DELAY_SECS, this.servingSpeedFactor);
		}
		return properties.asMap();
	}
}
