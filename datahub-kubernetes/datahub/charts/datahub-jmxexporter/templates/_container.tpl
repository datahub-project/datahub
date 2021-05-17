{{- define "datahub-jmxexporter.container" -}}
{{- if .Values.exporters.jmx.enabled }}
        - name: jmx-exporter
          image: "{{ .Values.exporters.jmx.image.repository }}:{{ .Values.exporters.jmx.image.tag }}"
          imagePullPolicy: {{ .Values.exporters.jmx.image.pullPolicy }}
          args: ["{{ .Values.exporters.jmx.ports.jmxxp.containerPort}}", "/opt/jmx_exporter/config.yml"]
          ports:
  {{- range $key, $port := .Values.exporters.jmx.ports }}
            - name: {{ $key }}
{{ toYaml $port | indent 14 }}
  {{- end }}
          livenessProbe:
{{ toYaml .Values.exporters.jmx.livenessProbe | indent 12 }}
          readinessProbe:
{{ toYaml .Values.exporters.jmx.readinessProbe | indent 12 }}
          env:
            - name: SERVICE_PORT
              value: {{ .Values.exporters.jmx.ports.jmxxp.containerPort | quote }}
          {{- with .Values.exporters.jmx.env }}
            {{- range $key, $value := . }}
            - name: {{ $key | upper | replace "." "_" }}
              value: {{ $value | quote }}
            {{- end }}
          {{- end }}
          resources:
{{ toYaml .Values.exporters.jmx.resources | indent 12 }}
          volumeMounts:
            - name: config-jmx-exporter
              mountPath: /opt/jmx_exporter/config.yml
              subPath: config.yml
{{- end }}
{{- end }}
