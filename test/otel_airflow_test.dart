// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_airflow/otel_airflow.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_airflow-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('dag_runs.create records dag + data interval + pool', () async {
    await tracedAirflow<String>(
      operation: 'dag_runs.create',
      invoke: () async => const AirflowCallResult(
        value: 'ok',
        result: AirflowResult(
          deploymentName: 'prod-mwaa',
          dagId: 'daily_warehouse_load',
          dagRunId: 'manual__2026-05-14T08:00:00',
          runType: 'manual',
          dataIntervalStart: '2026-05-13T00:00:00Z',
          dataIntervalEnd: '2026-05-14T00:00:00Z',
          dagRunState: 'queued',
          pool: 'warehouse_pool',
          queue: 'kubernetes',
          statusCode: 200,
        ),
      ),
    );
    final a = attrsOf(spans.findSpanByName('airflow dag_runs.create')!);
    expect(a['airflow.dag.id'], 'daily_warehouse_load');
    expect(a['airflow.run.type'], 'manual');
    expect(a['airflow.data_interval.end'], '2026-05-14T00:00:00Z');
    expect(a['airflow.pool'], 'warehouse_pool');
    expect(a['airflow.queue'], 'kubernetes');
  });

  test('failed task_instance sets span error', () async {
    await tracedAirflow<String>(
      operation: 'task_instances.list',
      invoke: () async => const AirflowCallResult(
        value: 'ok',
        result: AirflowResult(
          dagId: 'dag_1',
          taskId: 'load_snowflake',
          taskInstanceState: 'failed',
          tryNumber: 3,
          maxTries: 3,
          operatorName: 'SnowflakeOperator',
          durationSec: 42,
        ),
      ),
    );
    final s = spans.findSpanByName('airflow task_instances.list')!;
    expect(s.status, SpanStatusCode.Error);
    expect(attrsOf(s)['airflow.operator.name'], 'SnowflakeOperator');
  });

  test('up_for_retry does NOT set span error', () async {
    await tracedAirflow<String>(
      operation: 'task_instances.list',
      invoke: () async => const AirflowCallResult(
        value: 'ok',
        result: AirflowResult(
          taskInstanceState: 'up_for_retry',
          tryNumber: 1,
        ),
      ),
    );
    expect(
      spans.findSpanByName('airflow task_instances.list')!.status,
      isNot(SpanStatusCode.Error),
    );
  });

  test('suppression skips spans', () async {
    await runWithoutAirflowInstrumentationAsync(() async {
      await tracedAirflow<String>(
        operation: 'dags.list',
        invoke: () async => const AirflowCallResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('airflow '), isEmpty);
  });
}
