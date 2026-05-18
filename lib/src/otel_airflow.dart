// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_airflow_suppression.dart';

const _tracerName = 'dartastic_otel_libs_airflow';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

class AirflowResult {
  const AirflowResult({
    this.deploymentName,
    this.dagId,
    this.dagRunId,
    this.runType,
    this.executionDate,
    this.dataIntervalStart,
    this.dataIntervalEnd,
    this.dagRunState,
    this.taskId,
    this.taskInstanceState,
    this.tryNumber,
    this.maxTries,
    this.pool,
    this.queue,
    this.operatorName,
    this.durationSec,
    this.statusCode,
  });

  /// Astronomer/MWAA/self-hosted deployment id.
  final String? deploymentName;

  final String? dagId;
  final String? dagRunId;

  /// `scheduled` / `manual` / `backfill` / `dataset_triggered`.
  final String? runType;

  /// Legacy `execution_date` (the logical date the run represents,
  /// kept by old DAGs).
  final String? executionDate;

  /// Newer `data_interval_start` / `data_interval_end` (the
  /// time-window the DAG processes).
  final String? dataIntervalStart;
  final String? dataIntervalEnd;

  /// `queued` / `running` / `success` / `failed` /
  /// `up_for_retry` / `up_for_reschedule` / `removed` / `skipped`.
  final String? dagRunState;

  final String? taskId;

  /// `none` / `scheduled` / `queued` / `running` / `success` /
  /// `failed` / `up_for_retry` / `up_for_reschedule` / `removed` /
  /// `skipped` / `deferred` / `restarting`.
  final String? taskInstanceState;

  final int? tryNumber;
  final int? maxTries;

  /// Pool name — Airflow's concurrency-bucket.
  final String? pool;

  /// Celery/Kubernetes queue routing.
  final String? queue;

  /// `PythonOperator` / `BashOperator` / `KubernetesPodOperator` /
  /// `SnowflakeOperator` / `S3KeySensor` / etc.
  final String? operatorName;

  final int? durationSec;
  final int? statusCode;
}

class AirflowCallResult<T> {
  const AirflowCallResult({required this.value, this.result});
  final T value;
  final AirflowResult? result;
}

/// Wraps an Airflow REST API call in a CLIENT span. [operation] is
/// one of `dags.list`, `dags.get`, `dag_runs.create`,
/// `dag_runs.clear`, `dag_runs.get`, `task_instances.list`,
/// `task_instances.clear`, `task_instances.update`,
/// `pools.list`, `connections.list`, `variables.get`,
/// `datasets.list`.
Future<T> tracedAirflow<T>({
  required String operation,
  required Future<AirflowCallResult<T>> Function() invoke,
}) async {
  if (airflowInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'rpc.system': 'airflow',
    'rpc.method': operation,
    'airflow.operation': operation,
  };

  final span = _tracer().startSpan(
    'airflow $operation',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.result != null) {
      final extras = <Attribute<Object>>[];
      if (r.result!.deploymentName != null) {
        extras.add(OTel.attributeString(
          'airflow.deployment.name',
          r.result!.deploymentName!,
        ));
      }
      if (r.result!.dagId != null) {
        extras.add(OTel.attributeString(
          'airflow.dag.id',
          r.result!.dagId!,
        ));
      }
      if (r.result!.dagRunId != null) {
        extras.add(OTel.attributeString(
          'airflow.dag_run.id',
          r.result!.dagRunId!,
        ));
      }
      if (r.result!.runType != null) {
        extras.add(OTel.attributeString(
          'airflow.run.type',
          r.result!.runType!,
        ));
      }
      if (r.result!.executionDate != null) {
        extras.add(OTel.attributeString(
          'airflow.execution.date',
          r.result!.executionDate!,
        ));
      }
      if (r.result!.dataIntervalStart != null) {
        extras.add(OTel.attributeString(
          'airflow.data_interval.start',
          r.result!.dataIntervalStart!,
        ));
      }
      if (r.result!.dataIntervalEnd != null) {
        extras.add(OTel.attributeString(
          'airflow.data_interval.end',
          r.result!.dataIntervalEnd!,
        ));
      }
      if (r.result!.dagRunState != null) {
        extras.add(OTel.attributeString(
          'airflow.dag_run.state',
          r.result!.dagRunState!,
        ));
        if (r.result!.dagRunState == 'failed') {
          span.setStatus(SpanStatusCode.Error, 'Airflow dag_run failed');
        }
      }
      if (r.result!.taskId != null) {
        extras.add(OTel.attributeString(
          'airflow.task.id',
          r.result!.taskId!,
        ));
      }
      if (r.result!.taskInstanceState != null) {
        extras.add(OTel.attributeString(
          'airflow.task_instance.state',
          r.result!.taskInstanceState!,
        ));
        if (r.result!.taskInstanceState == 'failed') {
          span.setStatus(
            SpanStatusCode.Error,
            'Airflow task_instance failed',
          );
        }
      }
      if (r.result!.tryNumber != null) {
        extras.add(OTel.attributeInt(
          'airflow.try_number',
          r.result!.tryNumber!,
        ));
      }
      if (r.result!.maxTries != null) {
        extras.add(OTel.attributeInt(
          'airflow.max_tries',
          r.result!.maxTries!,
        ));
      }
      if (r.result!.pool != null) {
        extras.add(OTel.attributeString('airflow.pool', r.result!.pool!));
      }
      if (r.result!.queue != null) {
        extras.add(OTel.attributeString('airflow.queue', r.result!.queue!));
      }
      if (r.result!.operatorName != null) {
        extras.add(OTel.attributeString(
          'airflow.operator.name',
          r.result!.operatorName!,
        ));
      }
      if (r.result!.durationSec != null) {
        extras.add(OTel.attributeInt(
          'airflow.duration_sec',
          r.result!.durationSec!,
        ));
      }
      if (r.result!.statusCode != null) {
        extras.add(OTel.attributeInt(
          'http.response.status_code',
          r.result!.statusCode!,
        ));
        if (r.result!.statusCode! >= 400) {
          span.setStatus(
            SpanStatusCode.Error,
            'Airflow HTTP ${r.result!.statusCode}',
          );
        }
      }
      if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
