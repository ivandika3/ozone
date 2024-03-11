/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import {Table, Icon, Tooltip, Popover} from 'antd';
import {PaginationConfig} from 'antd/lib/pagination';
import moment from 'moment';
import {ReplicationIcon} from 'utils/themeIcons';
import StorageBar from 'components/storageBar/storageBar';
import {
  DatanodeState,
  DatanodeStateList,
  DatanodeOpState,
  DatanodeOpStateList,
  IStorageReport
} from 'types/datanode.types';
import './datanodes.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {MultiSelect, IOption} from 'components/multiSelect/multiSelect';
import {ActionMeta, ValueType} from 'react-select';
import {getCapacityPercent, showDataFetchError} from 'utils/common';
import {ColumnSearch} from 'utils/columnSearch';
import { AxiosGetHelper } from 'utils/axiosRequestHelper';
import {ColumnProps} from "antd/es/table";

interface IDatanodeResponse {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: string;
  storageReport: IStorageReport;
  pipelines: IPipeline[];
  containers: number;
  openContainers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
  networkLocation: string;
}

interface IDatanodesResponse {
  totalCount: number;
  datanodes: IDatanodeResponse[];
}

interface IDatanode {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: number;
  storageUsed: number;
  storageTotal: number;
  storageRemaining: number;
  storageCommitted: number;
  pipelines: IPipeline[];
  containers: number;
  openContainers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
  networkLocation: string;
}

interface IPipeline {
  pipelineID: string;
  replicationType: string;
  replicationFactor: string;
  leaderNode: string;
}

interface IDatanodesState {
  loading: boolean;
  dataSource: IDatanode[];
  totalCount: number;
  lastUpdated: number;
  selectedColumns: IOption[];
  columnOptions: IOption[];
  filteredInfo: Partial<Record<string, string[]>>;
}

const renderDatanodeState = (state: DatanodeState) => {
  const stateIconMap = {
    HEALTHY: <Icon type='check-circle' theme='filled' twoToneColor='#1da57a' className='icon-success'/>,
    STALE: <Icon type='hourglass' theme='filled' className='icon-warning'/>,
    DEAD: <Icon type='close-circle' theme='filled' className='icon-failure'/>
  };
  const icon = state in stateIconMap ? stateIconMap[state] : '';
  return <span>{icon} {state}</span>;
};

const renderDatanodeOpState = (opState: DatanodeOpState) => {
  const opStateIconMap = {
    IN_SERVICE: <Icon type='check-circle' theme='outlined' twoToneColor='#1da57a' className='icon-success'/>,
    DECOMMISSIONING: <Icon type='hourglass' theme='outlined' className='icon-warning'/>,
    DECOMMISSIONED: <Icon type='warning' theme='outlined' className='icon-warning'/>,
    ENTERING_MAINTENANCE: <Icon type='hourglass' theme='outlined' className='icon-warning'/>,
    IN_MAINTENANCE: <Icon type='warning' theme='outlined' className='icon-warning'/>
  };
  const icon = opState in opStateIconMap ? opStateIconMap[opState] : '';
  return <span>{icon} {opState}</span>;
};

const allColumnsOption: IOption = {
  label: 'Select all',
  value: '*'
};


const getTimeDiffFromTimestamp = (timestamp: number): string => {
  const timestampDate = new Date(timestamp);
  const currentDate = new Date();

  let elapsedTime = "";
  let duration: moment.Duration = moment.duration(
    moment(currentDate).diff(moment(timestampDate))
  )

  const durationKeys = ["seconds", "minutes", "hours", "days", "months", "years"]
  durationKeys.forEach((k) => {
    let time = duration["_data"][k]
    if (time !== 0){
      elapsedTime = time + `${k.substring(0, 1)} ` + elapsedTime
    }
  })

  return elapsedTime.trim().length === 0 ? "Just now" : elapsedTime.trim() + " ago";
}

let cancelSignal: AbortController;

export class Datanodes extends React.Component<Record<string, object>, IDatanodesState> {
  autoReload: AutoReloadHelper;
  columns: ColumnProps<IDatanode>[];
  defaultColumns: IOption[];

  constructor(props = {}) {
    super(props);
    this.columns = [
      {
        title: 'Hostname',
        dataIndex: 'hostname',
        key: 'hostname',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.hostname.localeCompare(b.hostname, undefined, {numeric: true}),
        defaultSortOrder: 'ascend' as const,
        fixed: 'left'
      },
      {
        title: 'State',
        dataIndex: 'state',
        key: 'state',
        isVisible: true,
        isSearchable: true,
        filterMultiple: true,
        filters: DatanodeStateList && DatanodeStateList.map(state => ({text: state, value: state})),
        onFilter: (value: DatanodeState, record: IDatanode) => record.state === value,
        render: (text: DatanodeState) => renderDatanodeState(text),
        sorter: (a: IDatanode, b: IDatanode) => a.state.localeCompare(b.state)
      },
      {
        title: 'Operational State',
        dataIndex: 'opState',
        key: 'opState',
        isVisible: true,
        isSearchable: true,
        filterMultiple: true,
        filters: DatanodeOpStateList && DatanodeOpStateList.map(state => ({text: state, value: state})),
        onFilter: (value: DatanodeOpState, record: IDatanode) => record.opState === value,
        render: (text: DatanodeOpState) => renderDatanodeOpState(text),
        sorter: (a: IDatanode, b: IDatanode) => a.opState.localeCompare(b.opState)
      },
      {
        title: 'Uuid',
        dataIndex: 'uuid',
        key: 'uuid',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.uuid.localeCompare(b.uuid),
        defaultSortOrder: 'ascend' as const
      },
      {
        title: 'Storage Capacity',
        dataIndex: 'storageCapacity',
        key: 'storageCapacity',
        isVisible: true,
        filters: [
          {
            text: 'Storage Remaining (default)',
            value: 'storageRemaining',
          },
          {
            text: 'Storage Used',
            value: 'storageUsed',
          },
          {
            text: 'Storage Committed',
            value: 'storageCommitted'
          },
          {
            text: 'Storage Total',
            value: 'storageTotal',
          },
          {
            text: 'Storage Utilization',
            value: 'storageUtilization',
          }
        ],
        filterIcon: () => (
          <Icon type={'sort-ascending'} />
        ),
        filterMultiple: false,
        onFilter: () => {
          // This filter is only used for placeholder, so return true regardless of the filter
          return true;
        },
        sorter: (a: IDatanode, b: IDatanode) => {
          let sortBy = "storageRemaining"
          if (this.state.filteredInfo.storageCapacity &&
              this.state.filteredInfo.storageCapacity.length > 0) {
            sortBy = this.state.filteredInfo.storageCapacity[0]
          }
          if (!sortBy) {
            return a.storageRemaining - b.storageRemaining
          }

          if (sortBy === "storageUsed") {
            return a.storageUsed - b.storageUsed;
          } else if (sortBy === "storageCommitted") {
            return a.storageCommitted - b.storageCommitted;
          } else if (sortBy === "storageTotal") {
            return a.storageTotal- b.storageTotal;
          } else if (sortBy === 'storageUtilization') {
            // See totalUsed calculation in storageBar.tsx
            return getCapacityPercent(a.storageTotal - a.storageRemaining, a.storageRemaining) -
                getCapacityPercent(b.storageTotal - b.storageRemaining, b.storageRemaining)
          } else {
            return a.storageRemaining - b.storageRemaining
          }
        },
        render: (text: string, record: IDatanode) => (
            <StorageBar
                total={record.storageTotal} used={record.storageUsed}
                remaining={record.storageRemaining} committed={record.storageCommitted}/>
        )},
      {
        title: 'Last Heartbeat',
        dataIndex: 'lastHeartbeat',
        key: 'lastHeartbeat',
        isVisible: true,
        sorter: (a: IDatanode, b: IDatanode) => a.lastHeartbeat - b.lastHeartbeat,
        render: (heartbeat: number) => {
          return heartbeat > 0 ? getTimeDiffFromTimestamp(heartbeat) : 'NA';
        }
      },
      {
        title: 'Pipeline ID(s)',
        dataIndex: 'pipelines',
        key: 'pipelines',
        isVisible: true,
        render: (pipelines: IPipeline[], record: IDatanode) => {
          let firstThreePipelinesIDs = [];
          let remainingPipelinesIDs: any[] = [];
          firstThreePipelinesIDs = pipelines && pipelines.filter((element, index) => index < 3);
          remainingPipelinesIDs = pipelines && pipelines.slice(3, pipelines.length);

          const RenderPipelineIds = ({ pipelinesIds }) => {
            return pipelinesIds && pipelinesIds.map((pipeline: any, index: any) => (
                <div key={index} className='pipeline-container'>
                  <ReplicationIcon
                      replicationFactor={pipeline.replicationFactor}
                      replicationType={pipeline.replicationType}
                      leaderNode={pipeline.leaderNode}
                      isLeader={pipeline.leaderNode === record.hostname} />
                  {pipeline.pipelineID}
                </div >
            ))
          }

          return (
              <>
                {
                  <RenderPipelineIds pipelinesIds={firstThreePipelinesIDs} />
                }
                {
                    remainingPipelinesIDs.length > 0 &&
                    <Popover content={<RenderPipelineIds pipelinesIds={remainingPipelinesIDs} />} title="Remaining pipelines" placement="rightTop" trigger="hover">
                      {`... and ${remainingPipelinesIDs.length} more pipelines`}
                    </Popover>
                }
              </>
          );
        }
      },
      {
        title:
            <span>
    Leader Count&nbsp;
              <Tooltip title='The number of Ratis Pipelines in which the given datanode is elected as a leader.'>
      <Icon type='info-circle'/>
    </Tooltip>
  </span>,
        dataIndex: 'leaderCount',
        key: 'leaderCount',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.leaderCount - b.leaderCount
      },
      {
        title: 'Containers',
        dataIndex: 'containers',
        key: 'containers',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.containers - b.containers
      },
      {
        title:
            <span>
    Open Containers&nbsp;
              <Tooltip title='The number of open containers per pipeline.'>
      <Icon type='info-circle'/>
    </Tooltip>
  </span>,
        dataIndex: 'openContainers',
        key: 'openContainers',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.openContainers - b.openContainers
      },
      {
        title: 'Version',
        dataIndex: 'version',
        key: 'version',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.version.localeCompare(b.version),
        defaultSortOrder: 'ascend' as const
      },
      {
        title: 'Setup Time',
        dataIndex: 'setupTime',
        key: 'setupTime',
        isVisible: true,
        sorter: (a: IDatanode, b: IDatanode) => a.setupTime - b.setupTime,
        render: (uptime: number) => {
          return uptime > 0 ? moment(uptime).format('ll LTS') : 'NA';
        }
      },
      {
        title: 'Revision',
        dataIndex: 'revision',
        key: 'revision',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.revision.localeCompare(b.revision),
        defaultSortOrder: 'ascend' as const
      },
      {
        title: 'Build Date',
        dataIndex: 'buildDate',
        key: 'buildDate',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.buildDate.localeCompare(b.buildDate),
        defaultSortOrder: 'ascend' as const
      },
      {
        title: 'Network Location',
        dataIndex: 'networkLocation',
        key: 'networkLocation',
        isVisible: true,
        isSearchable: true,
        sorter: (a: IDatanode, b: IDatanode) => a.networkLocation.localeCompare(b.networkLocation),
        defaultSortOrder: 'ascend' as const
      }
    ];

    this.defaultColumns = this.columns.map(column => ({
      label: column.key,
      value: column.key
    }));

    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0,
      lastUpdated: 0,
      selectedColumns: [],
      columnOptions: this.defaultColumns,
      filteredInfo: {},
    };


    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _handleChange = (pagination: PaginationConfig,
                   filters: Partial<Record<keyof IDatanode, string[]>>) => {
    this.setState({
      filteredInfo: filters,
    });
  }

  _handleColumnChange = (selected: ValueType<IOption, any>, _action: ActionMeta<IOption>) => {
    const selectedColumns = (selected == null ? [] : selected as IOption[]);
    this.setState({
      selectedColumns
    });
  };

  _getSelectedColumns = (selected: IOption[]) => {
    return selected.length > 0 ? selected : this.columns.filter(column => column.isVisible).map(column => ({
      label: column.key,
      value: column.key
    }));
  };

  _loadData = () => {
    this.setState(prevState => ({
      loading: true,
      selectedColumns: this._getSelectedColumns(prevState.selectedColumns)
    }));

    const { request, controller } = AxiosGetHelper('/api/v1/datanodes', cancelSignal);
    cancelSignal = controller;
    request.then(response => {
      const datanodesResponse: IDatanodesResponse = response.data;
      const totalCount = datanodesResponse.totalCount;
      const datanodes: IDatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: IDatanode[] = datanodes && datanodes.map(datanode => {
        return {
          hostname: datanode.hostname,
          uuid: datanode.uuid,
          state: datanode.state,
          opState: datanode.opState,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageRemaining: datanode.storageReport.remaining,
          storageCommitted: datanode.storageReport.committed,
          pipelines: datanode.pipelines,
          containers: datanode.containers,
          openContainers: datanode.openContainers,
          leaderCount: datanode.leaderCount,
          version: datanode.version,
          setupTime: datanode.setupTime,
          revision: datanode.revision,
          buildDate: datanode.buildDate,
          networkLocation: datanode.networkLocation
        };
      });

      this.setState({
        loading: false,
        dataSource,
        totalCount,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    // Fetch datanodes on component mount
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
    cancelSignal && cancelSignal.abort();
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  render() {
    const {dataSource, loading, totalCount, lastUpdated, selectedColumns, columnOptions} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} datanodes`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };

    return (
      <div className='datanodes-container'>
        <div className='page-header'>
          Datanodes ({totalCount})
          <div className='filter-block'>
            <MultiSelect
              allowSelectAll
              isMulti
              maxShowValues={3}
              className='multi-select-container'
              options={columnOptions}
              closeMenuOnSelect={false}
              hideSelectedOptions={false}
              value={selectedColumns}
              allOption={allColumnsOption}
              onChange={this._handleColumnChange}
            /> Columns
          </div>
          <AutoReloadPanel
            isLoading={loading}
            lastRefreshed={lastUpdated}
            togglePolling={this.autoReload.handleAutoReloadToggle}
            onReload={this._loadData}
          />
        </div>

        <div className='content-div'>
          <Table
            dataSource={dataSource}
            onChange={this._handleChange}
            columns={this.columns.reduce<any[]>((filtered, column) => {
              if (selectedColumns.some(e => e.value === column.key)) {
                if (column.isSearchable) {
                  const newColumn = {
                    ...column,
                    ...new ColumnSearch(column).getColumnSearchProps(column.dataIndex)
                  };
                  filtered.push(newColumn);
                } else {
                  filtered.push(column);
                }
              }

              return filtered;
            }, [])}
            loading={loading}
            pagination={paginationConfig}
            rowKey='hostname'
            scroll={{x: true, y: false, scrollToFirstRowOnChange: true}}
            locale={{filterTitle: ""}}
          />
        </div>
      </div>
    );
  }
}
