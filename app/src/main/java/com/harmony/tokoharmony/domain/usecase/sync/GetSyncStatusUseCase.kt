package com.harmony.tokoharmony.domain.usecase.sync

import com.harmony.tokoharmony.domain.model.SyncStatusInfo
import com.harmony.tokoharmony.domain.repository.SyncRepository
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
import javax.inject.Inject

class GetSyncStatusUseCase @Inject constructor(
    private val syncRepository: SyncRepository
) {
    operator fun invoke(): Flow<SyncStatusInfo> {
        val cal = java.util.Calendar.getInstance().apply {
            set(java.util.Calendar.HOUR_OF_DAY, 0)
            set(java.util.Calendar.MINUTE, 0)
            set(java.util.Calendar.SECOND, 0)
            set(java.util.Calendar.MILLISECOND, 0)
        }
        val startOfDay = cal.timeInMillis

        return combine(
            syncRepository.getPendingCount(),
            syncRepository.getFailedCount(),
            syncRepository.getLastSyncTime(),
            syncRepository.getSyncedTodayCount(startOfDay)
        ) { pending, failed, lastTime, syncedToday ->
            SyncStatusInfo(
                pendingCount = pending,
                failedCount = failed,
                lastSyncTime = lastTime,
                syncedTodayCount = syncedToday
            )
        }
    }
}
