//! Redis `KEYS` 스타일 glob 패턴 매칭
//!
//! * `*`  : 0개 이상의 임의 문자
//! * `?`  : 정확히 1개의 임의 문자
//! * `[abc]`, `[a-z]`, `[^a]`/`[!a]` : 문자 클래스
//! * `\x` : 특수문자 이스케이프

/// `text` 가 `pattern` 에 매칭되는지 검사합니다.
///
/// 마지막 `*` 위치만 기억해 두고 실패 시 그 지점부터 다시 시도하는
/// 그리디 + 백트래킹 방식이라 O(|pattern| * |text|) 입니다.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    // (마지막 '*' 의 패턴 위치, 그 '*' 가 현재까지 삼킨 텍스트 끝 위치)
    let mut backtrack: Option<(usize, usize)> = None;

    while ti < t.len() {
        if pi < p.len() {
            match p[pi] {
                '*' => {
                    backtrack = Some((pi, ti));
                    pi += 1;
                    continue;
                }
                '?' => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                '[' => match match_class(&p, pi, t[ti]) {
                    Some((true, next)) => {
                        pi = next;
                        ti += 1;
                        continue;
                    }
                    Some((false, _)) => {}
                    // 닫는 ']' 가 없으면 '[' 를 일반 문자로 취급
                    None => {
                        if t[ti] == '[' {
                            pi += 1;
                            ti += 1;
                            continue;
                        }
                    }
                },
                '\\' if pi + 1 < p.len() => {
                    if p[pi + 1] == t[ti] {
                        pi += 2;
                        ti += 1;
                        continue;
                    }
                }
                c => {
                    if c == t[ti] {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                }
            }
        }
        // 불일치: 직전 '*' 가 한 글자 더 삼키도록 하고 재시도
        match backtrack {
            Some((star_pi, star_ti)) => {
                pi = star_pi + 1;
                ti = star_ti + 1;
                backtrack = Some((star_pi, star_ti + 1));
            }
            None => return false,
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// `p[start] == '['` 인 문자 클래스를 해석합니다.
/// 반환: Some((매칭 여부, 클래스 다음 패턴 위치)), 닫는 ']' 가 없으면 None
fn match_class(p: &[char], start: usize, c: char) -> Option<(bool, usize)> {
    let mut i = start + 1;
    let negate = i < p.len() && (p[i] == '^' || p[i] == '!');
    if negate {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    while i < p.len() {
        if p[i] == ']' && !first {
            return Some((matched != negate, i + 1));
        }
        first = false;
        let mut lo = p[i];
        if lo == '\\' && i + 1 < p.len() {
            i += 1;
            lo = p[i];
        }
        if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
            let (mut hi, mut advance) = (p[i + 2], 3);
            if hi == '\\' && i + 3 < p.len() {
                hi = p[i + 3];
                advance = 4;
            }
            let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
            if lo <= c && c <= hi {
                matched = true;
            }
            i += advance;
        } else {
            if lo == c {
                matched = true;
            }
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn literals_and_wildcards() {
        assert!(glob_match("", ""));
        assert!(!glob_match("", "a"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:1"));
        assert!(glob_match("user:*", "user:"));
        assert!(!glob_match("user:*", "users:1"));
        assert!(glob_match("*:profile", "user:42:profile"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
        assert!(glob_match("h?llo", "hello"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn classes() {
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(!glob_match("h[ae]llo", "hillo"));
        assert!(glob_match("h[^e]llo", "hallo"));
        assert!(!glob_match("h[^e]llo", "hello"));
        assert!(glob_match("h[a-c]llo", "hbllo"));
        assert!(!glob_match("h[a-c]llo", "hdllo"));
        assert!(glob_match("[abc", "[abc"), "닫히지 않은 [ 는 문자 그대로");
    }

    #[test]
    fn escapes_and_unicode() {
        assert!(glob_match(r"a\*b", "a*b"));
        assert!(!glob_match(r"a\*b", "axb"));
        assert!(glob_match("사용자:*", "사용자:홍길동"));
        assert!(glob_match("??", "한글"));
    }
}
