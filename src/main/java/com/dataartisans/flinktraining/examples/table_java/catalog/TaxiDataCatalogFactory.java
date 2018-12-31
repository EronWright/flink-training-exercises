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

package com.dataartisans.flinktraining.examples.table_java.catalog;

import com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.ExternalCatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiData;

import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_FARES_FILE;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_MAX_EVENT_DELAY_SECS;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_RIDES_FILE;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_SERVING_SPEED_FACTOR;
import static com.dataartisans.flinktraining.examples.table_java.descriptors.TaxiDataValidator.CATALOG_TYPE_VALUE_TAXI_DATA;
import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_TYPE;

/**
 * An external catalog factory for {@link TaxiDataCatalog}.
 *
 * The table environment finds this catalog using descriptor properties provided by {@link TaxiData}.
 */
public class TaxiDataCatalogFactory implements ExternalCatalogFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_TAXI_DATA); // taxi-data
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CATALOG_RIDES_FILE);
		properties.add(CATALOG_FARES_FILE);
		properties.add(CATALOG_MAX_EVENT_DELAY_SECS);
		properties.add(CATALOG_SERVING_SPEED_FACTOR);
		return properties;
	}

	@Override
	public ExternalCatalog createExternalCatalog(Map<String, String> properties) {
		DescriptorProperties params = getValidatedProperties(properties);

		return new TaxiDataCatalog(
				params.getOptionalString(CATALOG_RIDES_FILE).orElse(null),
				params.getOptionalString(CATALOG_FARES_FILE).orElse(null),
				params.getOptionalInt(CATALOG_MAX_EVENT_DELAY_SECS).orElse(0),
				params.getOptionalInt(CATALOG_SERVING_SPEED_FACTOR).orElse(1)
		);
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new TaxiDataValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
