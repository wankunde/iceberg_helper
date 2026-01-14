// Keep globals: index.html relies on inline handlers in generated HTML (onclick="...").

// -------------------- globals --------------------
let currentMetadataDir = '';
let currentFileData = null;
let isTableVisible = false;
let allFiles = {}; // { metadata_files, snapshots, data_parquet }

// 预览页面返回栈
const viewHistory = [];

// NEW: cache schema field index from metadata.json:
// id -> { name, type }
let schemaFieldIndex = new Map();

// NEW: Iceberg system/builtin metadata columns (Integer.MAX_VALUE - n)
const INT_MAX = 0x7fffffff; // Integer.MAX_VALUE in Java
const builtinFieldIndex = new Map([
  [INT_MAX - 1, { name: '_file', type: 'string', doc: 'Path of the file in which a row is stored' }],
  [INT_MAX - 2, { name: '_pos', type: 'long', doc: 'Ordinal position of a row in the source data file' }],
  [INT_MAX - 3, { name: '_deleted', type: 'boolean', doc: 'Whether the row has been deleted' }],
  [INT_MAX - 4, { name: '_spec_id', type: 'int', doc: 'Spec ID used to track the file containing a row' }],
  [INT_MAX - 5, { name: '_partition', type: 'struct', doc: 'Partition to which a row belongs to' }],
  [INT_MAX - 6, { name: '_content_offset', type: 'long', doc: 'Content offset (Iceberg metadata column)' }],
  [INT_MAX - 7, { name: '_content_size_in_bytes', type: 'long', doc: 'Content size in bytes (Iceberg metadata column)' }],
  [2047483647, { name: '_row_number_column_file', type: 'long', doc: 'row number of column file' }],
]);

function _lookupFieldMetaById(idNum) {
  if (schemaFieldIndex && schemaFieldIndex.has(idNum)) return schemaFieldIndex.get(idNum);
  if (builtinFieldIndex && builtinFieldIndex.has(idNum)) return builtinFieldIndex.get(idNum);
  return null;
}

// -------------------- helpers: state/history --------------------
function _captureViewState() {
  const titleEl = document.getElementById('contentTitle');
  const codeEl = document.querySelector('#codeContent code');
  const tableContainer = document.getElementById('tableContainer');
  const overviewEl = document.getElementById('metadataOverview');
  const contentEl = document.getElementById('contentArea');
  const emptyEl = document.getElementById('emptyState');

  return {
    titleHTML: titleEl ? titleEl.innerHTML : '',
    codeText: codeEl ? codeEl.textContent : '',
    isTableHidden: tableContainer ? tableContainer.classList.contains('d-none') : true,
    tableHeadHTML: (document.getElementById('previewTableHead') || {}).innerHTML || '',
    tableBodyHTML: (document.getElementById('previewTableBody') || {}).innerHTML || '',
    isOverviewHidden: overviewEl ? overviewEl.classList.contains('d-none') : true,
    isContentHidden: contentEl ? contentEl.classList.contains('d-none') : true,
    isEmptyHidden: emptyEl ? emptyEl.classList.contains('d-none') : true,
  };
}

function _restoreViewState(state) {
  const titleEl = document.getElementById('contentTitle');
  const codeEl = document.querySelector('#codeContent code');
  const tableContainer = document.getElementById('tableContainer');
  const overviewEl = document.getElementById('metadataOverview');
  const contentEl = document.getElementById('contentArea');
  const emptyEl = document.getElementById('emptyState');

  if (titleEl) titleEl.innerHTML = state.titleHTML || '';
  if (codeEl) codeEl.textContent = state.codeText || '';

  const head = document.getElementById('previewTableHead');
  const body = document.getElementById('previewTableBody');
  if (head) head.innerHTML = state.tableHeadHTML || '';
  if (body) body.innerHTML = state.tableBodyHTML || '';
  if (tableContainer) tableContainer.classList.toggle('d-none', !!state.isTableHidden);

  if (overviewEl) overviewEl.classList.toggle('d-none', !!state.isOverviewHidden);
  if (contentEl) contentEl.classList.toggle('d-none', !!state.isContentHidden);
  if (emptyEl) emptyEl.classList.toggle('d-none', !!state.isEmptyHidden);

  if (codeEl && window.hljs && window.hljs.highlightElement) {
    window.hljs.highlightElement(codeEl);
  }
}

function _renderBackButtonIfNeeded() {
  const btnId = 'previewBackBtn';
  const host = document.getElementById('contentTitle');
  if (!host) return;

  const existing = document.getElementById(btnId);
  if (viewHistory.length === 0) {
    if (existing) existing.remove();
    return;
  }
  if (existing) return;

  const btn = document.createElement('button');
  btn.id = btnId;
  btn.className = 'btn btn-sm btn-outline-secondary ms-2';
  btn.type = 'button';
  btn.innerHTML = '<i class="bi bi-arrow-left"></i> 返回';
  btn.addEventListener('click', () => {
    const prev = viewHistory.pop();
    _restoreViewState(prev);
    _renderBackButtonIfNeeded();
  });

  host.insertAdjacentElement('beforeend', btn);
}

function _pushHistory() {
  viewHistory.push(_captureViewState());
  _renderBackButtonIfNeeded();
}

// -------------------- helpers: ui --------------------
function showLoading(show) {
  document.getElementById('loadingIndicator')?.classList.toggle('d-none', !show);
}

function showError(message) {
  const alert = document.getElementById('errorAlert');
  if (!alert) return;
  alert.textContent = message || '发生错误';
  alert.classList.remove('d-none');
}

function hideError() {
  document.getElementById('errorAlert')?.classList.add('d-none');
}

function showFileList() {
  document.getElementById('fileList')?.classList.remove('d-none');
}

function hideFileList() {
  document.getElementById('fileList')?.classList.add('d-none');
}

function showContent() {
  document.getElementById('contentArea')?.classList.remove('d-none');
  document.getElementById('emptyState')?.classList.add('d-none');
}

function hideContent() {
  document.getElementById('contentArea')?.classList.add('d-none');
}

function showOverview() {
  document.getElementById('metadataOverview')?.classList.remove('d-none');
}

function hideOverview() {
  document.getElementById('metadataOverview')?.classList.add('d-none');
}

// -------------------- formatting --------------------
function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function getFileIcon(type) {
  const icons = {
    metadata: 'bi bi-file-earmark-text text-info',
    snapshot: 'bi bi-camera text-primary',
    'data-avro': 'bi bi-file-earmark-binary text-success',
    'data-parquet': 'bi bi-table text-warning',
    json: 'bi bi-filetype-json text-warning',
    avro: 'bi bi-file-earmark-binary text-primary',
    other: 'bi bi-file text-secondary',
  };
  return icons[type] || icons.other;
}

// -------------------- content rendering --------------------
function displayContent(content, fileName, options) {
  options = options || {};
  const keepTable = !!options.keepTable;

  const titleEl = document.getElementById('contentTitle');
  if (titleEl) titleEl.innerHTML = `<i class="bi bi-file-text"></i> ${fileName || ''}`;

  const codeElement = document.querySelector('#codeContent code');
  if (codeElement) codeElement.textContent = content || '';

  const tableContainer = document.getElementById('tableContainer');
  if (tableContainer && !keepTable) {
    tableContainer.classList.add('d-none');
  }

  if (codeElement && window.hljs && window.hljs.highlightElement) {
    window.hljs.highlightElement(codeElement);
  } else if (codeElement) {
    codeElement.className = 'language-json';
  }

  isTableVisible = false;
  _renderBackButtonIfNeeded();
}

function displayPreviewTable(data) {
  const head = document.getElementById('previewTableHead');
  const body = document.getElementById('previewTableBody');
  const container = document.getElementById('tableContainer');
  if (!head || !body || !container) return;

  head.innerHTML = '';
  body.innerHTML = '';

  const fields = Array.isArray(data && data.fields) ? data.fields : [];
  const rows = Array.isArray(data && data.rows) ? data.rows : [];

  if (fields.length === 0) {
    container.classList.add('d-none');
    return;
  }

  const headerRow = document.createElement('tr');
  fields.forEach((f) => {
    const th = document.createElement('th');
    th.textContent = f;
    headerRow.appendChild(th);
  });
  head.appendChild(headerRow);

  rows.forEach((r) => {
    const tr = document.createElement('tr');
    fields.forEach((f) => {
      const td = document.createElement('td');
      const v = r && typeof r === 'object' ? r[f] : '';
      td.textContent = v === undefined || v === null ? '' : String(v);
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });

  container.classList.remove('d-none');
  isTableVisible = true;
}

// -------------------- overview rendering --------------------
function _setOverviewMode(mode) {
  const schemaCard = document.getElementById('schemaFieldsCard');
  const manifestBox = document.getElementById('manifestDataFiles');
  const tbody = document.getElementById('schemaFieldsTableBody');
  const countEl = document.getElementById('schemaFieldsCount');

  if (schemaCard) schemaCard.classList.toggle('d-none', mode !== 'metadata');
  if (manifestBox) manifestBox.classList.toggle('d-none', mode !== 'manifest');

  // snapshot/manifest 默认不展示 schema 表格内容
  if (mode !== 'metadata') {
    if (countEl) countEl.textContent = '0';
    if (tbody) {
      tbody.innerHTML = `
        <tr>
          <td colspan="4" class="text-muted">${mode === 'snapshot' ? 'Snapshot view' : 'Manifest view'} (no schema fields)</td>
        </tr>
      `;
    }
  }
}

function displayMetadataOverview(info) {
  _setOverviewMode('metadata');

  const metricsEl = document.getElementById('overviewMetrics');
  const tbody = document.getElementById('schemaFieldsTableBody');
  const countEl = document.getElementById('schemaFieldsCount');

  const legacyCards = document.getElementById('overviewCards');

  if (legacyCards) legacyCards.innerHTML = '';
  if (metricsEl) metricsEl.innerHTML = '';
  if (tbody) tbody.innerHTML = '';

  const metrics = [
    { title: 'Format Version', value: info?.format_version ?? 'N/A', icon: 'bi-123' },
    { title: 'Current Snapshot ID', value: info?.current_snapshot_id ?? 'N/A', icon: 'bi-camera' },
    { title: 'Snapshots Count', value: Array.isArray(info?.snapshots) ? info.snapshots.length : (info?.snapshots_count ?? 0), icon: 'bi-stack' },
  ];

  if (metricsEl) {
    metrics.forEach((m) => {
      metricsEl.insertAdjacentHTML(
        'beforeend',
        `
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi ${m.icon}"></i> ${m.title}</h6>
            <p class="card-text text-break">${m.value}</p>
          </div>
        </div>
        `
      );
    });

    const props = (info && typeof info === 'object' && info.properties && typeof info.properties === 'object')
      ? info.properties
      : {};

    const entries = Object.entries(props || {});
    const rowsHtml = entries.length
      ? entries
          .sort(([a], [b]) => String(a).localeCompare(String(b)))
          .map(([k, v]) => {
            const val = (typeof v === 'string') ? v : JSON.stringify(v);
            return `
              <tr>
                <td class="text-muted text-break" style="width: 45%;">${k}</td>
                <td class="text-break">${val ?? ''}</td>
              </tr>
            `;
          })
          .join('')
      : `<tr><td colspan="2" class="text-muted">No properties</td></tr>`;

    metricsEl.insertAdjacentHTML(
      'beforeend',
      `
      <div class="card metadata-card">
        <div class="card-body">
          <div class="d-flex justify-content-between align-items-center mb-2">
            <h6 class="card-title mb-0"><i class="bi bi-sliders"></i> Properties</h6>
            <span class="badge bg-secondary">${entries.length}</span>
          </div>
          <div class="table-responsive properties-table">
            <table class="table table-sm table-striped table-bordered mb-0">
              <thead>
                <tr>
                  <th>key</th>
                  <th>value</th>
                </tr>
              </thead>
              <tbody>
                ${rowsHtml}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      `
    );
  }

  const fields = Array.isArray(info?.schema?.fields) ? info.schema.fields : [];
  if (countEl) countEl.textContent = String(fields.length);

  if (tbody) {
    if (fields.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="4" class="text-muted">No schema fields</td>
        </tr>
      `;
    } else {
      fields.forEach((f) => {
        const id = f?.id ?? '';
        const name = f?.name ?? '';
        const required = f?.required ?? false;
        const type = typeof f?.type === 'string' ? f.type : JSON.stringify(f?.type ?? '');

        tbody.insertAdjacentHTML(
          'beforeend',
          `
          <tr>
            <td>${id}</td>
            <td class="text-break">${name}</td>
            <td>${String(required)}</td>
            <td class="text-break"><code>${type}</code></td>
          </tr>
          `
        );
      });
    }
  }
}

function displaySnapshotInfo(info, manifestPaths, fileName) {
  _setOverviewMode('snapshot');

  const metricsEl = document.getElementById('overviewMetrics');
  const tbody = document.getElementById('schemaFieldsTableBody');
  const countEl = document.getElementById('schemaFieldsCount');
  const legacyCards = document.getElementById('overviewCards');

  if (legacyCards) legacyCards.innerHTML = '';
  if (metricsEl) metricsEl.innerHTML = '';

  // Right side placeholder
  if (countEl) countEl.textContent = '0';
  if (tbody) {
    tbody.innerHTML = `
      <tr>
        <td colspan="4" class="text-muted">Snapshot view (no schema fields)</td>
      </tr>
    `;
  }

  if (!metricsEl) {
    showOverview();
    return;
  }

  // 1) Added Snapshot ID (full width)
  metricsEl.insertAdjacentHTML(
    'beforeend',
    `
    <div class="card metadata-card">
      <div class="card-body">
        <h6 class="card-title"><i class="bi bi-plus-circle"></i> Added Snapshot ID</h6>
        <p class="card-text text-break">${info?.added_snapshot_id ?? 'N/A'}</p>
      </div>
    </div>
    `
  );

  // 2) Data Files row (3 cols)
  metricsEl.insertAdjacentHTML(
    'beforeend',
    `
    <div class="row g-2">
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-file-plus"></i> Added Data Files</h6>
            <p class="card-text">${info?.added_data_files_count ?? 0}</p>
          </div>
        </div>
      </div>
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-file-check"></i> Existing Data Files</h6>
            <p class="card-text">${info?.existing_data_files_count ?? 0}</p>
          </div>
        </div>
      </div>
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-file-minus"></i> Deleted Data Files</h6>
            <p class="card-text">${info?.deleted_data_files_count ?? 0}</p>
          </div>
        </div>
      </div>
    </div>
    `
  );

  // 3) Rows row (3 cols)
  metricsEl.insertAdjacentHTML(
    'beforeend',
    `
    <div class="row g-2">
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-plus"></i> Added Rows</h6>
            <p class="card-text">${info?.added_rows_count ?? 0}</p>
          </div>
        </div>
      </div>
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-check"></i> Existing Rows</h6>
            <p class="card-text">${info?.existing_rows_count ?? 0}</p>
          </div>
        </div>
      </div>
      <div class="col-12 col-md-4">
        <div class="card metadata-card">
          <div class="card-body">
            <h6 class="card-title"><i class="bi bi-dash"></i> Deleted Rows</h6>
            <p class="card-text">${info?.deleted_rows_count ?? 0}</p>
          </div>
        </div>
      </div>
    </div>
    `
  );

  showOverview();
}

// NEW: render column_file_ids as a field table using schemaFieldIndex
function _renderColumnIdTable(columnFileIds) {
  const ids = Array.isArray(columnFileIds) ? columnFileIds : [];
  if (ids.length === 0) return '<small class="text-muted">无 column_file_ids</small>';

  const rows = ids
    .map((rawId) => {
      const idNum = Number(rawId);
      const meta = _lookupFieldMetaById(idNum);
      const name = meta?.name ?? 'N/A';
      const type = meta?.type ?? 'N/A';
      return `
        <tr>
          <td style="width:72px;">${rawId}</td>
          <td class="text-break">${name}</td>
          <td class="text-break"><code>${type}</code></td>
        </tr>
      `;
    })
    .join('');

  return `
    <div class="table-responsive mt-2">
      <table class="table table-sm table-striped table-bordered mb-0">
        <thead>
          <tr>
            <th style="width:72px;">id</th>
            <th>name</th>
            <th>type</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function _renderColumnFilesForManifest(columnFiles) {
  if (!columnFiles || columnFiles.length === 0) return '<small class="text-muted">无 column_files</small>';

  let html = '<ul class="list-group">';
  columnFiles.forEach((cf) => {
    html += `
      <li class="list-group-item">
        <div class="row">
          <div class="col-md-12">
            <strong>column_file_path:</strong> <span class="text-break">${cf.column_file_path || 'N/A'}</span>
          </div>
          <div class="col-md-3"><strong>length:</strong> ${cf.column_file_length ?? 'N/A'}</div>
          <div class="col-md-3"><strong>record_count:</strong> ${cf.column_file_record_count ?? 'N/A'}</div>
          <div class="col-md-3"><strong>snapshot_id:</strong> ${cf.column_file_snapshot_id ?? 'N/A'}</div>

          <!-- CHANGED: show friendly field table instead of raw ids -->
          <div class="col-md-12">
            <strong>fields:</strong>
            ${_renderColumnIdTable(cf.column_file_ids || [])}
          </div>

          <div class="col-md-12 mt-2">
            <button class="btn btn-sm btn-outline-primary"
              data-file-path="${cf.column_file_path || ''}"
              data-file-format=""
              onclick="previewDataFileFromButton(this)">
              <i class="bi bi-eye"></i> 预览前100行
            </button>
          </div>
        </div>
      </li>
    `;
  });
  html += '</ul>';
  return html;
}

function displayManifestInfo(info, fileName) {
  _setOverviewMode('manifest');

  const metricsEl = document.getElementById('overviewMetrics');
  const manifestBox = document.getElementById('manifestDataFiles');
  const legacyCards = document.getElementById('overviewCards');

  if (legacyCards) legacyCards.innerHTML = '';
  if (metricsEl) metricsEl.innerHTML = '';
  if (manifestBox) manifestBox.innerHTML = '';

  if (metricsEl) {
    metricsEl.insertAdjacentHTML(
      'beforeend',
      `
      <div class="card metadata-card">
        <div class="card-body">
          <h6 class="card-title"><i class="bi bi-file-earmark-binary"></i> Manifest 文件</h6>
          <p class="card-text text-break">${fileName || 'N/A'}</p>
        </div>
      </div>
      `
    );

    metricsEl.insertAdjacentHTML(
      'beforeend',
      `
      <div class="card metadata-card">
        <div class="card-body">
          <h6 class="card-title"><i class="bi bi-list-ol"></i> 条目数</h6>
          <p class="card-text text-break">${info?.entries_count ?? 0}</p>
        </div>
      </div>
      `
    );

    metricsEl.insertAdjacentHTML(
      'beforeend',
      `
      <div class="card metadata-card">
        <div class="card-body">
          <h6 class="card-title"><i class="bi bi-file-earmark-text"></i> Data Files</h6>
          <p class="card-text text-break">${(info?.data_files || []).length}</p>
        </div>
      </div>
      `
    );
  }

  if (manifestBox) {
    const dataFiles = info?.data_files || [];
    if (dataFiles.length === 0) {
      manifestBox.innerHTML = `<div class="card metadata-card"><div class="card-body text-muted">No data files</div></div>`;
    } else {
      let html = '';
      dataFiles.forEach((df, idx) => {
        html += `
          <div class="card snapshot-info-card">
            <div class="card-body">
              <h6 class="card-title"><i class="bi bi-file-earmark-text"></i> Data File #${idx + 1}</h6>

              <div class="row">
                <div class="col-md-12">
                  <strong>file_path:</strong> <span class="text-break">${df.file_path || 'N/A'}</span>
                </div>
                <div class="col-md-3"><strong>file_format:</strong> ${df.file_format || 'N/A'}</div>
                <div class="col-md-3"><strong>record_count:</strong> ${df.record_count ?? 'N/A'}</div>
                <div class="col-md-3"><strong>file_size_in_bytes:</strong> ${df.file_size_in_bytes ?? 'N/A'}</div>
                <div class="col-md-12"><strong>partition:</strong> <span class="text-break">${JSON.stringify(df.partition || {})}</span></div>
              </div>

              <div class="mt-2">
                <button class="btn btn-sm btn-outline-primary"
                  data-file-path="${df.file_path || ''}"
                  data-file-format="${df.file_format || ''}"
                  onclick="previewDataFileFromButton(this)">
                  <i class="bi bi-eye"></i> 预览前100行
                </button>
              </div>

              <div class="mt-3">
                <h6><i class="bi bi-columns"></i> Column Files</h6>
                ${_renderColumnFilesForManifest(df.column_files || [])}
              </div>
            </div>
          </div>
        `;
      });
      manifestBox.innerHTML = html;
    }
  }

  showOverview();
}

// -------------------- file list rendering --------------------
function displayMetadataTree(containerId, metadataFiles, latestVersion) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = '';

  if (!metadataFiles || metadataFiles.length === 0) {
    container.innerHTML = '<small class="text-muted">无文件</small>';
    return;
  }

  metadataFiles.forEach((metadataFile) => {
    const div = document.createElement('div');
    div.className = 'tree-item';
    div.dataset.path = metadataFile.path;
    div.dataset.type = 'metadata';
    div.dataset.name = metadataFile.name;

    const isLatest = latestVersion && metadataFile.name === latestVersion;
    const icon = getFileIcon('metadata');
    const badge = isLatest ? '<span class="badge bg-success file-type-badge ms-1">最新</span>' : '';

    div.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <span class="tree-toggle collapsed"></span>
          <i class="${icon}"></i> ${metadataFile.name}${badge}
        </div>
        <small class="file-size">${formatFileSize(metadataFile.size)}</small>
      </div>
    `;

    const childrenDiv = document.createElement('div');
    childrenDiv.className = 'tree-children';
    childrenDiv.id = `metadata-${metadataFile.name.replace(/[^a-zA-Z0-9]/g, '-')}-children`;

    div.addEventListener('click', async (e) => {
      if (e.target.classList.contains('tree-toggle') || e.target.parentElement?.classList.contains('tree-toggle')) {
        const toggle = div.querySelector('.tree-toggle');
        const isExpanded = toggle.classList.contains('expanded');
        toggle.classList.toggle('expanded', !isExpanded);
        toggle.classList.toggle('collapsed', isExpanded);
        childrenDiv.classList.toggle('expanded', !isExpanded);

        if (!isExpanded && childrenDiv.children.length === 0) {
          await loadSnapshotsForMetadata(metadataFile.path, childrenDiv);
        }
        return;
      }

      document.querySelectorAll('.tree-item').forEach((item) => item.classList.remove('active'));
      div.classList.add('active');

      await loadFile(metadataFile.path, 'json', metadataFile.name);
      await loadMetadataInfo(metadataFile.path, 'json');
    });

    container.appendChild(div);
    container.appendChild(childrenDiv);
  });
}

function displayFileGroup(containerId, files, latestVersion, fileType) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = '';

  if (!files || files.length === 0) {
    container.innerHTML = '<small class="text-muted">无文件</small>';
    return;
  }

  files.forEach((file) => {
    const div = document.createElement('div');
    div.className = 'file-item';
    div.dataset.path = file.path;
    div.dataset.type = fileType;
    div.dataset.name = file.name;

    const icon = getFileIcon(fileType);
    div.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div><i class="${icon}"></i> ${file.name}</div>
        <small class="file-size">${formatFileSize(file.size)}</small>
      </div>
    `;

    div.addEventListener('click', () => {
      document.querySelectorAll('.file-item').forEach((item) => item.classList.remove('active'));
      div.classList.add('active');

      if (fileType === 'data-parquet') {
        previewDataFile(file.path, 'parquet');
      }
    });

    container.appendChild(div);
  });
}

function displayFileList(files, latestVersion) {
  allFiles = {
    metadata_files: files.metadata_files || [],
    snapshots: files.snapshots || [],
    data_parquet: files.data_parquet || [],
  };

  displayMetadataTree('metadataFiles', allFiles.metadata_files, latestVersion);
  displayFileGroup('dataParquet', allFiles.data_parquet, null, 'data-parquet');

  if (latestVersion) document.getElementById('latestVersionBadge')?.classList.remove('d-none');
}

// -------------------- API calls --------------------
function _toMetadataDirFromTableRoot(inputPath) {
  const p = String(inputPath || '').trim().replace(/\/+$/, '');
  if (!p) return '';
  // 如果用户已经输入的是 metadata 目录，保持不变
  if (p.endsWith('/metadata')) return p;
  return p + '/metadata';
}

async function loadDirectory(path) {
  showLoading(true);
  hideError();
  hideFileList();
  hideContent();
  hideOverview();

  try {
    // CHANGED: UI 输入为 table root，这里自动补 /metadata
    const metadataDir = _toMetadataDirFromTableRoot(path);

    const response = await fetch(`/api/list-dir?path=${encodeURIComponent(metadataDir)}`);
    const result = await response.json();

    if (!result.success) {
      showError(result.error || '加载目录失败');
      return;
    }

    // NOTE: currentMetadataDir 仍保存实际 metadata 目录
    currentMetadataDir = metadataDir;

    displayFileList(result.files, result.latest_version);
    showFileList();

    if (result.latest_version) {
      const latestFile = (result.files.metadata_files || []).find((f) => f.name === result.latest_version);
      if (latestFile) {
        await loadFile(latestFile.path, 'json', latestFile.name);
        await loadMetadataInfo(latestFile.path, 'json');
      }
    }
  } catch (e) {
    showError(`加载失败: ${e.message}`);
  } finally {
    showLoading(false);
  }
}

async function loadFile(filePath, fileType, fileName) {
  showLoading(true);
  hideContent();

  try {
    const endpoint = fileType === 'avro' ? '/api/avro' : '/api/json';
    const response = await fetch(`${endpoint}?file_path=${encodeURIComponent(filePath)}&formatted=true`);
    const result = await response.json();

    if (!result.success) {
      showError(result.error || '加载文件失败');
      return;
    }

    currentFileData = result;
    displayContent(result.formatted || JSON.stringify(result.data, null, 2), fileName);
    showContent();
  } catch (e) {
    showError(`加载文件失败: ${e.message}`);
  } finally {
    showLoading(false);
  }
}

async function loadMetadataInfo(filePath, fileType) {
  try {
    const response = await fetch(`/api/metadata-info?file_path=${encodeURIComponent(filePath)}&file_type=${fileType}`);
    const result = await response.json();
    if (result.success) {
      // NEW: refresh schema index for manifest rendering
      schemaFieldIndex = new Map();
      const fields = result?.info?.schema?.fields;
      if (Array.isArray(fields)) {
        fields.forEach((f) => {
          const id = f?.id;
          if (id === undefined || id === null) return;
          const name = f?.name ?? '';
          const type = typeof f?.type === 'string' ? f.type : JSON.stringify(f?.type ?? '');
          schemaFieldIndex.set(Number(id), { name, type });
        });
      }

      displayMetadataOverview(result.info);
      showOverview();
    }
  } catch (e) {
    // keep silent but observable in console
    console.error('加载元数据概览失败:', e);
  }
}

async function loadSnapshotsForMetadata(metadataPath, container) {
  try {
    const response = await fetch(`/api/metadata/current-manifests?file_path=${encodeURIComponent(metadataPath)}`);
    const result = await response.json();
    if (!result.success) {
      container.innerHTML = '<small class="text-muted">无法加载 snapshot 信息</small>';
      return;
    }

    const manifestList = result.manifest_list;
    const manifestPaths = result.manifest_paths || [];
    if (!manifestList) {
      container.innerHTML = '<small class="text-muted">未找到 manifest-list</small>';
      return;
    }

    const actualPath = String(manifestList).replace(/^file:/, '');
    const fileName = actualPath.split('/').pop() || String(manifestList);

    const snapshotDiv = document.createElement('div');
    snapshotDiv.className = 'tree-item';
    snapshotDiv.dataset.path = actualPath;
    snapshotDiv.dataset.type = 'snapshot';
    snapshotDiv.dataset.name = fileName;
    const icon = getFileIcon('snapshot');

    snapshotDiv.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <span class="tree-toggle collapsed"></span>
          <i class="${icon}"></i> ${fileName}
        </div>
        <small class="file-size">Avro</small>
      </div>
    `;

    const manifestChildrenDiv = document.createElement('div');
    manifestChildrenDiv.className = 'tree-children';

    snapshotDiv.addEventListener('click', async (e) => {
      if (e.target.classList.contains('tree-toggle') || e.target.parentElement?.classList.contains('tree-toggle')) {
        const toggle = snapshotDiv.querySelector('.tree-toggle');
        const isExpanded = toggle.classList.contains('expanded');
        toggle.classList.toggle('expanded', !isExpanded);
        toggle.classList.toggle('collapsed', isExpanded);
        manifestChildrenDiv.classList.toggle('expanded', !isExpanded);

        if (!isExpanded && manifestChildrenDiv.children.length === 0) {
          await loadManifestsForSnapshot(manifestPaths, manifestChildrenDiv);
        }
        return;
      }

      document.querySelectorAll('.tree-item').forEach((item) => item.classList.remove('active'));
      snapshotDiv.classList.add('active');
      await loadSnapshotFile(actualPath, fileName);
    });

    container.appendChild(snapshotDiv);
    container.appendChild(manifestChildrenDiv);
  } catch (e) {
    container.innerHTML = `<small class="text-danger">加载失败: ${e.message}</small>`;
  }
}

async function loadManifestsForSnapshot(manifestPaths, container) {
  if (!manifestPaths || manifestPaths.length === 0) {
    container.innerHTML = '<small class="text-muted">未找到 manifest 文件</small>';
    return;
  }
  manifestPaths.forEach((manifestPath) => {
    const fileName = manifestPath.split('/').pop() || manifestPath;
    const actualPath = manifestPath.replace(/^file:/, '');

    const manifestDiv = document.createElement('div');
    manifestDiv.className = 'tree-item';
    manifestDiv.dataset.path = actualPath;
    manifestDiv.dataset.type = 'manifest';
    manifestDiv.dataset.name = fileName;

    manifestDiv.innerHTML = `
      <div class="d-flex justify-content-between align-items-center">
        <div><i class="bi bi-link-45deg"></i> ${fileName}</div>
        <small class="file-size">Manifest</small>
      </div>
    `;

    manifestDiv.addEventListener('click', async (e) => {
      e.stopPropagation();
      document.querySelectorAll('.tree-item').forEach((item) => item.classList.remove('active'));
      manifestDiv.classList.add('active');
      await loadManifestFile(actualPath, fileName);
    });

    container.appendChild(manifestDiv);
  });
}

async function loadSnapshotFile(filePath, fileName) {
  showLoading(true);
  hideContent();
  hideOverview();

  try {
    const response = await fetch(`/api/metadata/snapshot?file_path=${encodeURIComponent(filePath)}`);
    const result = await response.json();
    if (!result.success) {
      showError(result.error || '加载 Snapshot 文件失败');
      return;
    }

    currentFileData = result;
    displaySnapshotInfo(result.info, result.manifest_paths, fileName);
    displayContent(result.formatted || JSON.stringify(result.snapshot, null, 2), fileName);
    showContent();
  } catch (e) {
    showError(`加载 Snapshot 文件失败: ${e.message}`);
  } finally {
    showLoading(false);
  }
}

async function loadManifestFile(filePath, fileName) {
  showLoading(true);
  hideContent();
  hideOverview();

  try {
    const response = await fetch(`/api/metadata/manifest?file_path=${encodeURIComponent(filePath)}`);
    const result = await response.json();
    if (!result.success) {
      showError(result.error || '加载 Manifest 文件失败');
      return;
    }
    currentFileData = result;
    displayManifestInfo(result.info, fileName);
    displayContent(result.formatted || JSON.stringify(result.manifest, null, 2), fileName);
    showOverview();
    showContent();
  } catch (e) {
    showError(`加载 Manifest 文件失败: ${e.message}`);
  } finally {
    showLoading(false);
  }
}

async function previewDataFile(filePath, fileFormat) {
  try {
    _pushHistory();

    const actualPath = String(filePath || '').replace(/^file:/, '');
    const fmt = (fileFormat || '').toLowerCase();
    const url = `/api/preview/datafile?file_path=${encodeURIComponent(actualPath)}${fmt ? `&file_format=${fmt}` : ''}&limit=100`;

    showLoading(true);
    hideOverview();

    const response = await fetch(url);
    const result = await response.json();
    if (!result.success) {
      showError(result.error || '预览数据文件失败');
      return;
    }

    currentFileData = result;
    const name = actualPath.split('/').pop() || actualPath;

    displayPreviewTable(result.data);
    displayContent(result.formatted || JSON.stringify(result.data, null, 2), name + ' 预览', { keepTable: true });
    showContent();
    _renderBackButtonIfNeeded();
  } catch (e) {
    showError(`预览数据文件失败: ${e.message}`);
  } finally {
    showLoading(false);
  }
}

function previewDataFileFromButton(btn) {
  const path = btn.getAttribute('data-file-path') || '';
  const fmt = btn.getAttribute('data-file-format') || '';
  previewDataFile(path, fmt);
}

// -------------------- boot --------------------
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('loadBtn')?.addEventListener('click', async () => {
    const path = document.getElementById('metadataPath')?.value?.trim();
    if (!path) {
      showError('请输入目录路径');
      return;
    }
    await loadDirectory(path);
  });

  document.getElementById('metadataPath')?.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') document.getElementById('loadBtn')?.click();
  });

  document.getElementById('copyBtn')?.addEventListener('click', () => {
    const content = document.querySelector('#codeContent code')?.textContent || '';
    navigator.clipboard.writeText(content).then(() => {
      const btn = document.getElementById('copyBtn');
      if (!btn) return;
      const original = btn.innerHTML;
      btn.innerHTML = '<i class="bi bi-check"></i> 已复制';
      btn.classList.add('btn-success');
      btn.classList.remove('btn-outline-secondary');
      setTimeout(() => {
        btn.innerHTML = original;
        btn.classList.remove('btn-success');
        btn.classList.add('btn-outline-secondary');
      }, 1200);
    });
  });

  document.getElementById('searchInput')?.addEventListener('input', (e) => {
    const term = (e.target.value || '').toLowerCase();
    const codeElement = document.querySelector('#codeContent code');
    if (!codeElement) return;

    if (!term) {
      const text = codeElement.textContent;
      codeElement.innerHTML = '';
      codeElement.textContent = text;
      if (window.hljs && window.hljs.highlightElement) window.hljs.highlightElement(codeElement);
      return;
    }

    const text = codeElement.textContent || '';
    const regex = new RegExp(`(${term})`, 'gi');
    codeElement.innerHTML = text.replace(regex, '<mark>$1</mark>');
  });

  // 自动加载默认路径
  const defaultPath = document.getElementById('metadataPath')?.value?.trim();
  if (defaultPath) setTimeout(() => loadDirectory(defaultPath), 200);
});
