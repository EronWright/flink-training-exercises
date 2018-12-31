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
import org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator;

/**
 * A validator for {@link TaxiData}.
 */
public class TaxiDataValidator extends ExternalCatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_TAXI_DATA = "taxi-data";
	public static final String CATALOG_RIDES_FILE = "rides-file";
	public static final String CATALOG_FARES_FILE = "fares-file";
	public static final String CATALOG_MAX_EVENT_DELAY_SECS = "max-event-delay-secs";
	public static final String CATALOG_SERVING_SPEED_FACTOR = "serving-speed-factor";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_TAXI_DATA, false);
		properties.validateString(CATALOG_RIDES_FILE, true, 1);
		properties.validateString(CATALOG_FARES_FILE, true, 1);
		properties.validateInt(CATALOG_MAX_EVENT_DELAY_SECS, true, 0);
		properties.validateInt(CATALOG_SERVING_SPEED_FACTOR, true, 1);
	}
}
