// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_airflow/otel_airflow.dart';

/// Traces Airflow calls with OpenTelemetry.
///
/// `otel_airflow` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Airflow call in
/// [tracedAirflow]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'airflow-example');

  // Wraps an Airflow REST API call in a CLIENT span.
  final airflowId = await tracedAirflow<String>(
    operation: 'dag_runs.create',
    invoke: () async {
      // Make the Airflow call here, then report what it returned.
      const id = 'airflow_8f3a2c';
      return const AirflowCallResult(
          value: id,
          result: AirflowResult(
              deploymentName: 'example',
              dagId: 'dag_8f3a2c',
              dagRunId: 'dagrun_8f3a2c',
              runType: 'scheduled',
              executionDate: 'execution_date',
              dataIntervalStart: 'data_interval_start',
              dataIntervalEnd: 'example',
              dagRunState: 'queued',
              taskId: 'task_8f3a2c',
              taskInstanceState: 'none',
              tryNumber: 1,
              maxTries: 1,
              pool: 'example',
              queue: 'orders',
              operatorName: 'PythonOperator',
              durationSec: 120,
              statusCode: 201));
    },
  );
  OTelLog.info('tracedAirflow dag_runs.create returned $airflowId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutAirflowInstrumentationAsync.
  await runWithoutAirflowInstrumentationAsync(
    () => tracedAirflow<String>(
      operation: 'dag_runs.create',
      invoke: () async {
        const id = 'airflow_8f3a2c';
        return const AirflowCallResult(
            value: id,
            result: AirflowResult(
                deploymentName: 'example',
                dagId: 'dag_8f3a2c',
                dagRunId: 'dagrun_8f3a2c',
                runType: 'scheduled',
                executionDate: 'execution_date',
                dataIntervalStart: 'data_interval_start',
                dataIntervalEnd: 'example',
                dagRunState: 'queued',
                taskId: 'task_8f3a2c',
                taskInstanceState: 'none',
                tryNumber: 1,
                maxTries: 1,
                pool: 'example',
                queue: 'orders',
                operatorName: 'PythonOperator',
                durationSec: 120,
                statusCode: 201));
      },
    ),
  );

  await OTel.shutdown();
}
